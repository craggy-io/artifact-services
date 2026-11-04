// Package runs coordinates work around a caller-supplied function: sessions, private
// staging of outputs, publication and lineage in an idempotent order, pinned reads,
// and deterministic reuse. It composes public layer APIs and adds no privilege. It
// executes nothing itself and sandboxes nothing.
//
// # Commit protocol
//
// The layers have no shared transaction. Instead each step has a stable ID, so a
// crash anywhere is repaired by running the identical job again:
//
//  1. Start record (run/<id>/start). A run that dies leaves this with no end record,
//     which is how crashed work stays visibly unfinished.
//  2. The function runs. Blobs go to CAS and trees to a spool: nothing is registered.
//  3. Outputs are published as content snapshots. No occurrence or lineage refers to
//     them yet, so they are not yet a result.
//  4. The completion record commits. This is the run's one commit point: it names the
//     outputs and everything still to be derived from it. For a deterministic job its
//     ID is fixed by the derivation key, so among racing executors exactly one
//     succeeds. The others find a conflict and adopt the winner's outputs.
//  5. Occurrences, derived-from edges and planned assertions are recorded from the
//     completion record. Anyone who finds the completion record can redo this step,
//     so a crash here converges too.
//
// A loser's outputs that differ from the winner's remain as unreferenced snapshots:
// never named by an occurrence or an edge, but visible to raw catalog search.
package runs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/crashpoint"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/provenance"
)

// Activity kinds and relationship kinds this package writes.
const (
	KindSessionStart = "runs.session.start"
	KindSessionEnd   = "runs.session.end"
	KindRunStart     = "runs.run.start"
	KindRunEnd       = "runs.run.end"
	KindDerivedFrom  = "runs.derived-from"
)

// Outcomes recorded on end records.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
)

var (
	// ErrUndeclaredRead reports a strict run reading what it did not declare. It is a
	// check that keeps derivation keys honest, not a security boundary: the function
	// could still read through a service handle it holds.
	ErrUndeclaredRead = errors.New("runs: read of undeclared input")
	// ErrInvalid reports a job or session that cannot be recorded.
	ErrInvalid = errors.New("runs: invalid request")
	// ErrPriorFailure reports a run ID that has already ended in failure. A failure
	// is not retried under the same ID: its record is immutable. Use a new ID.
	ErrPriorFailure = errors.New("runs: run already failed")
)

// Options binds a coordinator to the services it composes. Assertions is optional
// unless a job plans assertions.
type Options struct {
	CAS        cas.Store
	Artifacts  *artifacts.Service
	Lineage    *lineage.Service
	Assertions *assertions.Service
}

type flight struct {
	done chan struct{}
	err  error
}

// Coordinator is safe for concurrent use. Its in-process suppression of identical
// deterministic runs is only an optimization: the commit protocol stays correct
// among coordinators that share services.
type Coordinator struct {
	options  Options
	mu       sync.Mutex
	inflight map[string]*flight
}

// New checks that the required services are present.
func New(options Options) (*Coordinator, error) {
	if options.CAS == nil || options.Artifacts == nil || options.Lineage == nil {
		return nil, fmt.Errorf("%w: CAS, Artifacts and Lineage are required", ErrInvalid)
	}
	return &Coordinator{options: options, inflight: map[string]*flight{}}, nil
}

func mintID(prefix string) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(random[:]), nil
}

func metadataOf(fields map[string]any) (map[string]json.RawMessage, error) {
	metadata := map[string]json.RawMessage{}
	for key, value := range fields {
		if value == nil || value == "" {
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("%w: metadata %s: %v", ErrInvalid, key, err)
		}
		metadata[key] = encoded
	}
	return metadata, nil
}

func readField[T any](metadata map[string]json.RawMessage, key string) (T, error) {
	var value T
	raw, ok := metadata[key]
	if !ok {
		return value, nil
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, fmt.Errorf("runs: stored metadata %s: %w", key, err)
	}
	return value, nil
}

// SessionRequest starts a scope of related work. An empty ID is minted. Empty Actor
// and Owner inherit from Parent. They are opaque labels: this package authenticates
// nothing.
type SessionRequest struct {
	ID    string
	Actor string
	Owner string
	Label string
}

// Session is a scope with an actor and owner, started by a record written first and
// ended by another. If the process dies between them it stays visibly unfinished.
type Session struct {
	coordinator *Coordinator
	id          string
	actor       string
	owner       string
}

// ID returns the session's identity.
func (s *Session) ID() string { return s.id }

// Actor returns the session's actor.
func (s *Session) Actor() string { return s.actor }

// Owner returns the session's owner.
func (s *Session) Owner() string { return s.owner }

func sessionStart(id string) lineage.ID { return lineage.ID("session/" + id + "/start") }
func sessionEnd(id string) lineage.ID   { return lineage.ID("session/" + id + "/end") }
func runStart(id string) lineage.ID     { return lineage.ID("run/" + id + "/start") }
func runEnd(id string) lineage.ID       { return lineage.ID("run/" + id + "/end") }
func derivationRecord(key string) lineage.ID {
	return lineage.ID("runs/d/" + key)
}

// Begin records a session's start before returning it.
func (c *Coordinator) Begin(ctx context.Context, request SessionRequest) (*Session, error) {
	return c.begin(ctx, request, "")
}

func (c *Coordinator) begin(ctx context.Context, request SessionRequest, parent string) (*Session, error) {
	if request.ID == "" {
		minted, err := mintID("")
		if err != nil {
			return nil, err
		}
		request.ID = minted
	}
	metadata, err := metadataOf(map[string]any{"owner": request.Owner, "label": request.Label, "parent": parent})
	if err != nil {
		return nil, err
	}
	record := lineage.Record{ID: sessionStart(request.ID), Activity: &lineage.Activity{Kind: KindSessionStart, Actor: request.Actor, Metadata: metadata}}
	if _, err := c.options.Lineage.Record(ctx, record); err != nil {
		return nil, err
	}
	return &Session{coordinator: c, id: request.ID, actor: request.Actor, owner: request.Owner}, nil
}

// Child begins a nested session, inheriting actor and owner where the request has
// none. Children end independently of their parent.
func (s *Session) Child(ctx context.Context, request SessionRequest) (*Session, error) {
	if request.Actor == "" {
		request.Actor = s.actor
	}
	if request.Owner == "" {
		request.Owner = s.owner
	}
	return s.coordinator.begin(ctx, request, s.id)
}

// End records success, or failure with cause's text. It is idempotent for an
// identical outcome and ErrConflict for a different one.
func (s *Session) End(ctx context.Context, cause error) error {
	outcome, message := OutcomeSucceeded, ""
	if cause != nil {
		outcome, message = OutcomeFailed, cause.Error()
	}
	metadata, err := metadataOf(map[string]any{"outcome": outcome, "error": message})
	if err != nil {
		return err
	}
	_, err = s.coordinator.options.Lineage.Record(ctx, lineage.Record{ID: sessionEnd(s.id), Activity: &lineage.Activity{Kind: KindSessionEnd, Actor: s.actor, Metadata: metadata}})
	return err
}

// Code identifies what ran. Artifact pins code stored as an artifact. Together with
// params and input content it decides a deterministic run's identity, so anything
// that can change its output must be in here.
type Code struct {
	Kind     string       `json:"kind"`
	Name     string       `json:"name"`
	Version  string       `json:"version,omitempty"`
	Artifact artifacts.ID `json:"artifact,omitempty"`
}

// Job describes one run. An empty ID is minted, and then a retry cannot be recognised
// as the same run: pass a stable ID to make a run resumable. Deterministic declares
// that output depends only on Code, Params and Inputs, which is the caller's
// judgement, and implies Strict. Strict rejects reads of undeclared inputs; otherwise
// such reads are recorded as extra inputs. Writer signs any planned assertions.
type Job struct {
	ID            string
	Code          Code
	Inputs        []provenance.Ref
	Params        json.RawMessage
	Deterministic bool
	Strict        bool
	Writer        assertions.Writer
}

// Output is one published result. Occurrence is empty only for a failed run whose
// finalization did not happen.
type Output struct {
	Name       string                 `json:"name"`
	Artifact   artifacts.ID           `json:"artifact"`
	Occurrence artifacts.OccurrenceID `json:"occurrence"`
}

// Result is a run's outcome. Reused means this call did not produce the outputs: it
// found them from an earlier or concurrent run of the same job, whose ID Run names.
type Result struct {
	Run        string
	Completion lineage.ID
	Outputs    []Output
	Reused     bool
}

// Output returns the named output.
func (r Result) Output(name string) (Output, bool) {
	for _, o := range r.Outputs {
		if o.Name == name {
			return o, true
		}
	}
	return Output{}, false
}

// RunError reports a failed function along with what it kept, if anything.
type RunError struct {
	Run     string
	Outputs []Output
	Err     error
}

func (e *RunError) Error() string { return fmt.Sprintf("runs: run %s failed: %v", e.Run, e.Err) }
func (e *RunError) Unwrap() error { return e.Err }

// Pins are the journal positions a run reads as of, fixed at its start.
type Pins struct {
	Artifacts  uint64 `json:"artifacts"`
	Lineage    uint64 `json:"lineage"`
	Assertions uint64 `json:"assertions,omitempty"`
}

// DerivationKey computes a job's reuse key from code, canonical params and the content
// of its inputs. Equal keys are the claim that outputs may be shared.
func (c *Coordinator) DerivationKey(ctx context.Context, job Job) (string, error) {
	inputs := make([]string, 0, len(job.Inputs))
	for _, ref := range job.Inputs {
		identity, err := c.contentIdentity(ctx, ref)
		if err != nil {
			return "", err
		}
		inputs = append(inputs, identity)
	}
	sort.Strings(inputs)
	var params json.RawMessage
	if len(job.Params) > 0 {
		normalized, err := canon.Normalize(job.Params)
		if err != nil {
			return "", fmt.Errorf("%w: params: %v", ErrInvalid, err)
		}
		params = normalized
	}
	encoded, err := canon.Marshal(struct {
		Code   Code            `json:"code"`
		Params json.RawMessage `json:"params,omitempty"`
		Inputs []string        `json:"inputs"`
	}{job.Code, params, inputs})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// contentIdentity reduces a reference to what its content is, not where it was found:
// an occurrence and the snapshot it publishes are the same input, and a revision's
// name does not matter once its artifact is pinned.
func (c *Coordinator) contentIdentity(ctx context.Context, ref provenance.Ref) (string, error) {
	if err := ref.Validate(); err != nil {
		return "", fmt.Errorf("%w: input: %v", ErrInvalid, err)
	}
	if ref.Occurrence != "" {
		occurrence, err := c.options.Artifacts.GetOccurrence(ctx, artifacts.OccurrenceID(ref.Occurrence))
		if err != nil {
			return "", err
		}
		ref.Artifact, ref.Occurrence = string(occurrence.Artifact), ""
	}
	if ref.Revision != "" {
		record, err := c.options.Lineage.Get(ctx, lineage.ID(ref.Revision))
		if err != nil {
			return "", err
		}
		if record.Revision == nil {
			return "", fmt.Errorf("%w: revision input %s", ErrInvalid, ref.Revision)
		}
		ref.Artifact, ref.Revision = string(record.Revision.Artifact), ""
	}
	if ref.Artifact != "" && ref.Path != "" {
		info, err := c.options.Artifacts.Stat(ctx, artifacts.ID(ref.Artifact), ref.Path)
		if err != nil {
			return "", err
		}
		if info.Content != "" {
			ref.Artifact, ref.Path, ref.Digest = "", "", string(info.Content)
		}
	}
	encoded, err := canon.Marshal(ref)
	return string(encoded), err
}

// Run executes fn under the commit protocol. See the package documentation.
func (s *Session) Run(ctx context.Context, job Job, fn func(context.Context, *Run) error) (Result, error) {
	c := s.coordinator
	if job.Code.Kind == "" || job.Code.Name == "" {
		return Result{}, fmt.Errorf("%w: code needs a kind and name", ErrInvalid)
	}
	if job.Deterministic {
		job.Strict = true
	}
	if job.ID == "" {
		minted, err := mintID("")
		if err != nil {
			return Result{}, err
		}
		job.ID = minted
	}
	if len(job.ID) > 128 || !provenance.ValidRecordID(job.ID) {
		return Result{}, fmt.Errorf("%w: run ID", ErrInvalid)
	}
	if !job.Deterministic {
		return s.execute(ctx, job, "", fn)
	}
	key, err := c.DerivationKey(ctx, job)
	if err != nil {
		return Result{}, err
	}
	for {
		if result, found, err := c.adopt(ctx, s, key, job); found || err != nil {
			return result, err
		}
		c.mu.Lock()
		call, running := c.inflight[key]
		if !running {
			call = &flight{done: make(chan struct{})}
			c.inflight[key] = call
			c.mu.Unlock()
			result, err := s.execute(ctx, job, key, fn)
			c.mu.Lock()
			delete(c.inflight, key)
			c.mu.Unlock()
			call.err = err
			close(call.done)
			return result, err
		}
		c.mu.Unlock()
		select {
		case <-call.done:
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
}

// adopt returns an earlier completed run with this derivation key, finishing any
// finalization it left undone. If this job's own ID had started and died before
// ending, that attempt is closed against the result it now shares.
func (c *Coordinator) adopt(ctx context.Context, s *Session, key string, job Job) (Result, bool, error) {
	record, err := c.options.Lineage.Get(ctx, derivationRecord(key))
	if errors.Is(err, lineage.ErrNotFound) {
		return Result{}, false, nil
	}
	if err != nil {
		return Result{}, false, err
	}
	if _, err := c.options.Lineage.Get(ctx, runStart(job.ID)); err == nil {
		if err := c.endDerived(ctx, s, job.ID, record.ID); err != nil && !errors.Is(err, lineage.ErrConflict) {
			return Result{}, true, err
		}
	} else if !errors.Is(err, lineage.ErrNotFound) {
		return Result{}, true, err
	}
	result, err := c.finalize(ctx, record, job.Writer)
	result.Reused = true
	return result, true, err
}

func (s *Session) execute(ctx context.Context, job Job, key string, fn func(context.Context, *Run) error) (Result, error) {
	c := s.coordinator
	if key == "" {
		// A plain run with a stable ID is resumable: a finished one is not repeated.
		record, err := c.options.Lineage.Get(ctx, runEnd(job.ID))
		if err == nil {
			return c.resume(ctx, record, job)
		}
		if !errors.Is(err, lineage.ErrNotFound) {
			return Result{}, err
		}
	}
	pins, err := c.start(ctx, s, job, key)
	if err != nil {
		return Result{}, err
	}
	crashpoint.Hit(crashpoint.RunStarted)
	run := &Run{coordinator: c, job: job, pins: pins, key: key, stages: map[string]*stage{}, declared: map[provenance.Ref]bool{}, session: s}
	for _, ref := range job.Inputs {
		run.declared[ref] = true
	}
	defer run.discard()
	failure := fn(ctx, run)
	if failure != nil {
		return c.fail(ctx, s, run, failure)
	}
	return c.complete(ctx, s, run)
}

// resume answers a repeated stable run ID from its end record.
func (c *Coordinator) resume(ctx context.Context, record lineage.Record, job Job) (Result, error) {
	outcome, err := readField[string](record.Activity.Metadata, "outcome")
	if err != nil {
		return Result{}, err
	}
	if outcome == OutcomeFailed {
		message, _ := readField[string](record.Activity.Metadata, "error")
		outputs, _ := readField[[]Output](record.Activity.Metadata, "outputs")
		return Result{Run: job.ID, Completion: record.ID, Outputs: outputs}, &RunError{Run: job.ID, Outputs: outputs, Err: fmt.Errorf("%w: %s", ErrPriorFailure, message)}
	}
	result, err := c.finalize(ctx, record, job.Writer)
	result.Reused = true
	return result, err
}

func (c *Coordinator) sequences(ctx context.Context) (Pins, error) {
	var pins Pins
	var err error
	if pins.Artifacts, err = c.options.Artifacts.Sequence(ctx); err != nil {
		return pins, err
	}
	if pins.Lineage, err = c.options.Lineage.Sequence(ctx); err != nil {
		return pins, err
	}
	if c.options.Assertions != nil {
		if pins.Assertions, err = c.options.Assertions.Sequence(ctx); err != nil {
			return pins, err
		}
	}
	return pins, nil
}

// start writes the start record unless a crashed attempt already did, in which case
// its pins stand: a resumed run keeps reading the state it started from.
func (c *Coordinator) start(ctx context.Context, s *Session, job Job, key string) (Pins, error) {
	existing, err := c.options.Lineage.Get(ctx, runStart(job.ID))
	if err == nil {
		return readField[Pins](existing.Activity.Metadata, "pins")
	}
	if !errors.Is(err, lineage.ErrNotFound) {
		return Pins{}, err
	}
	pins, err := c.sequences(ctx)
	if err != nil {
		return Pins{}, err
	}
	metadata, err := metadataOf(map[string]any{"code": job.Code, "session": s.id, "owner": s.owner, "pins": pins, "derivation": key, "deterministic": job.Deterministic})
	if err != nil {
		return Pins{}, err
	}
	if len(job.Params) > 0 {
		metadata["params"] = job.Params
	}
	_, err = c.options.Lineage.Record(ctx, lineage.Record{ID: runStart(job.ID), Activity: &lineage.Activity{Kind: KindRunStart, Actor: s.actor, Inputs: job.Inputs, Metadata: metadata}})
	return pins, err
}

// plannedAssertion is an assertion recorded in finalization, persisted in the
// completion record without its credential, which is not storable.
type plannedAssertion struct {
	ID       assertions.ID     `json:"id"`
	Output   string            `json:"output,omitempty"`
	Kind     assertions.KindID `json:"kind"`
	Subject  provenance.Ref    `json:"subject,omitempty"`
	Value    json.RawMessage   `json:"value"`
	Evidence []provenance.Ref  `json:"evidence,omitempty"`
	Key      string            `json:"key,omitempty"`
	Replaces assertions.ID     `json:"replaces,omitempty"`
}

func occurrenceFor(completion lineage.ID, name string) artifacts.OccurrenceID {
	sum := sha256.Sum256([]byte(string(completion) + "\x00" + name))
	return artifacts.OccurrenceID("occ-" + hex.EncodeToString(sum[:16]))
}

func (c *Coordinator) publishStages(ctx context.Context, run *Run, keep func(string) bool) ([]Output, error) {
	names := make([]string, 0, len(run.stages))
	for name := range run.stages {
		if keep(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	outputs := make([]Output, 0, len(names))
	for _, name := range names {
		id, err := run.stages[name].publish(ctx)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, Output{Name: name, Artifact: id})
	}
	return outputs, nil
}

func (r *Run) allInputs() []provenance.Ref {
	inputs := append([]provenance.Ref(nil), r.job.Inputs...)
	for ref := range r.reads {
		if !r.declared[ref] {
			inputs = append(inputs, ref)
		}
	}
	sort.Slice(inputs, func(i, j int) bool {
		a, _ := json.Marshal(inputs[i])
		b, _ := json.Marshal(inputs[j])
		return string(a) < string(b)
	})
	return inputs
}

func (c *Coordinator) complete(ctx context.Context, s *Session, run *Run) (Result, error) {
	outputs, err := c.publishStages(ctx, run, func(string) bool { return true })
	if err != nil {
		return Result{}, err
	}
	crashpoint.Hit(crashpoint.RunOutputsPublished)
	id := runEnd(run.job.ID)
	if run.key != "" {
		id = derivationRecord(run.key)
	}
	refs := make([]provenance.Ref, 0, len(outputs))
	for i := range outputs {
		outputs[i].Occurrence = occurrenceFor(id, outputs[i].Name)
		refs = append(refs, provenance.Ref{Artifact: string(outputs[i].Artifact)})
	}
	planned := make([]plannedAssertion, len(run.planned))
	for i, p := range run.planned {
		if p.ID == "" {
			sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d", id, i))
			p.ID = assertions.ID("as-" + hex.EncodeToString(sum[:16]))
		}
		planned[i] = p
	}
	metadata, err := metadataOf(map[string]any{"run": run.job.ID, "outcome": OutcomeSucceeded, "outputs": outputs, "session": s.id, "derivation": run.key})
	if err != nil {
		return Result{}, err
	}
	if len(planned) > 0 {
		if metadata["assertions"], err = json.Marshal(planned); err != nil {
			return Result{}, err
		}
	}
	record := lineage.Record{ID: id, Activity: &lineage.Activity{Kind: KindRunEnd, Actor: s.actor, Inputs: run.allInputs(), Outputs: refs, Metadata: metadata}}
	_, err = c.options.Lineage.Record(ctx, record)
	reused := false
	if err != nil {
		if run.key == "" || !errors.Is(err, lineage.ErrConflict) {
			return Result{}, err
		}
		// Another executor won this derivation. Its record is the result.
		reused = true
		if record, err = c.options.Lineage.Get(ctx, id); err != nil {
			return Result{}, err
		}
	}
	crashpoint.Hit(crashpoint.RunCompleted)
	if run.key != "" {
		// The run's own end record links its start to the shared derivation record, so
		// the start is not left looking unfinished, whichever executor won.
		if err := c.endDerived(ctx, s, run.job.ID, id); err != nil {
			return Result{}, err
		}
	}
	result, err := c.finalize(ctx, record, run.job.Writer)
	result.Reused = reused
	if err == nil {
		crashpoint.Hit(crashpoint.RunFinalized)
	}
	return result, err
}

func (c *Coordinator) endDerived(ctx context.Context, s *Session, run string, completion lineage.ID) error {
	metadata, err := metadataOf(map[string]any{"run": run, "outcome": OutcomeSucceeded, "completion": string(completion)})
	if err != nil {
		return err
	}
	_, err = c.options.Lineage.Record(ctx, lineage.Record{ID: runEnd(run), Activity: &lineage.Activity{Kind: KindRunEnd, Actor: s.actor, Metadata: metadata}})
	return err
}

// finalize records everything derived from a completion record. All of it has stable
// IDs, so it is safe to run by anyone, any number of times.
func (c *Coordinator) finalize(ctx context.Context, record lineage.Record, writer assertions.Writer) (Result, error) {
	activity := record.Activity
	run, err := readField[string](activity.Metadata, "run")
	if err != nil {
		return Result{}, err
	}
	completion, err := readField[string](activity.Metadata, "completion")
	if err != nil {
		return Result{}, err
	}
	if completion != "" {
		// A derived end record points at the derivation record holding the outputs.
		shared, err := c.options.Lineage.Get(ctx, lineage.ID(completion))
		if err != nil {
			return Result{}, err
		}
		return c.finalize(ctx, shared, writer)
	}
	outputs, err := readField[[]Output](activity.Metadata, "outputs")
	if err != nil {
		return Result{}, err
	}
	result := Result{Run: run, Completion: record.ID, Outputs: outputs}
	for _, output := range outputs {
		if _, err := c.options.Artifacts.RecordOccurrence(ctx, artifacts.OccurrenceRequest{ID: output.Occurrence, Artifact: output.Artifact, Name: output.Name, Producer: string(record.ID)}); err != nil {
			return result, err
		}
	}
	index := 0
	for _, output := range outputs {
		for _, input := range activity.Inputs {
			edge := lineage.Record{ID: lineage.ID(fmt.Sprintf("%s/edge/%d", record.ID, index)), Relationship: &lineage.Relationship{
				Kind: KindDerivedFrom, From: provenance.Ref{Occurrence: string(output.Occurrence)}, To: input, Activity: record.ID,
			}}
			index++
			if _, err := c.options.Lineage.Record(ctx, edge); err != nil {
				return result, err
			}
		}
	}
	planned, err := readField[[]plannedAssertion](activity.Metadata, "assertions")
	if err != nil {
		return result, err
	}
	for _, p := range planned {
		if err := c.recordPlanned(ctx, p, outputs, string(record.ID), writer); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (c *Coordinator) recordPlanned(ctx context.Context, p plannedAssertion, outputs []Output, activity string, writer assertions.Writer) error {
	if c.options.Assertions == nil {
		return fmt.Errorf("%w: job plans assertions but the coordinator has no assertion service", ErrInvalid)
	}
	subject := p.Subject
	if p.Output != "" {
		found := slices.IndexFunc(outputs, func(o Output) bool { return o.Name == p.Output })
		if found < 0 {
			return fmt.Errorf("%w: assertion about unknown output %q", ErrInvalid, p.Output)
		}
		subject = provenance.Ref{Occurrence: string(outputs[found].Occurrence)}
	}
	_, err := c.options.Assertions.Record(ctx, assertions.Request{ID: p.ID, Kind: p.Kind, Subject: subject, Value: p.Value, Evidence: p.Evidence, Key: p.Key, Replaces: p.Replaces, Activity: activity, Writer: writer})
	return err
}

func (c *Coordinator) fail(ctx context.Context, s *Session, run *Run, failure error) (Result, error) {
	outputs, err := c.publishStages(ctx, run, func(name string) bool { return run.kept[name] })
	if err != nil {
		return Result{}, errors.Join(failure, err)
	}
	id := runEnd(run.job.ID)
	refs := make([]provenance.Ref, 0, len(outputs))
	for i := range outputs {
		outputs[i].Occurrence = occurrenceFor(id, outputs[i].Name)
		refs = append(refs, provenance.Ref{Artifact: string(outputs[i].Artifact)})
	}
	metadata, err := metadataOf(map[string]any{"run": run.job.ID, "outcome": OutcomeFailed, "error": failure.Error(), "outputs": outputs, "session": s.id})
	if err != nil {
		return Result{}, errors.Join(failure, err)
	}
	record := lineage.Record{ID: id, Activity: &lineage.Activity{Kind: KindRunEnd, Actor: s.actor, Inputs: run.allInputs(), Outputs: refs, Metadata: metadata}}
	if _, err := c.options.Lineage.Record(ctx, record); err != nil {
		return Result{}, errors.Join(failure, err)
	}
	result, err := c.finalize(ctx, record, assertions.Writer{})
	if err != nil {
		return result, errors.Join(failure, err)
	}
	return result, &RunError{Run: run.job.ID, Outputs: outputs, Err: failure}
}

// Unfinished is work whose start record has no end record: still running, or crashed.
type Unfinished struct {
	ID    string
	Run   bool
	Actor string
}

// Unfinished lists sessions and runs with a start record and no end record. It scans
// lineage for start records, and its cost grows with their number.
func (c *Coordinator) Unfinished(ctx context.Context) ([]Unfinished, error) {
	var found []Unfinished
	for _, kind := range []string{KindSessionStart, KindRunStart} {
		cursor := ""
		for {
			page, err := c.options.Lineage.List(ctx, lineage.Query{ActivityKind: kind, Limit: 1000, Cursor: cursor})
			if err != nil {
				return nil, err
			}
			for _, record := range page.Records {
				id, end := startedWork(record.ID)
				if _, err := c.options.Lineage.Get(ctx, end); errors.Is(err, lineage.ErrNotFound) {
					found = append(found, Unfinished{ID: id, Run: kind == KindRunStart, Actor: record.Activity.Actor})
				} else if err != nil {
					return nil, err
				}
			}
			if cursor = page.NextCursor; cursor == "" {
				break
			}
		}
	}
	return found, nil
}

func startedWork(start lineage.ID) (string, lineage.ID) {
	trimmed := strings.TrimSuffix(string(start), "/start")
	_, id, _ := strings.Cut(trimmed, "/")
	return id, lineage.ID(trimmed + "/end")
}

// stage is one staged output: how to publish it, and what to release if it never is.
type stage struct {
	publish func(context.Context) (artifacts.ID, error)
	close   func() error
}

// Run is the handle a job's function works through. It is not safe for concurrent use
// by the function unless stated.
type Run struct {
	coordinator *Coordinator
	session     *Session
	job         Job
	key         string
	pins        Pins
	stages      map[string]*stage
	kept        map[string]bool
	declared    map[provenance.Ref]bool
	reads       map[provenance.Ref]bool
	planned     []plannedAssertion
}

// ID returns the run's identity.
func (r *Run) ID() string { return r.job.ID }

// Pins returns the journal positions this run reads as of.
func (r *Run) Pins() Pins { return r.pins }

func (r *Run) discard() {
	for _, s := range r.stages {
		if s.close != nil {
			_ = s.close()
		}
	}
}

func (r *Run) addStage(name string, s *stage) error {
	if _, exists := r.stages[name]; exists {
		return fmt.Errorf("%w: output %q staged twice", ErrInvalid, name)
	}
	r.stages[name] = s
	return nil
}

// Blob stages one file under an output name, which is also its path in the published
// snapshot. The bytes reach CAS now but nothing refers to them until success.
func (r *Run) Blob(ctx context.Context, name string, content io.Reader, mode uint32) error {
	put, err := r.coordinator.options.CAS.Put(ctx, content)
	if err != nil {
		return err
	}
	entry := artifacts.Entry{Path: name, Content: put.ID, Size: put.Size, Mode: mode}
	return r.addStage(name, &stage{publish: func(ctx context.Context) (artifacts.ID, error) {
		return r.coordinator.options.Artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{entry}})
	}})
}

// TreeStage collects a tree's entries until the run decides to publish it.
type TreeStage struct{ builder *artifacts.Builder }

// Add queues one entry; see artifacts.Builder.Add.
func (t *TreeStage) Add(e artifacts.Entry) error { return t.builder.Add(e) }

// Tree stages a tree of any size under an output name. Entries refer to content
// already in CAS, typically stored with Run.Content.
func (r *Run) Tree(name string) (*TreeStage, error) {
	builder := r.coordinator.options.Artifacts.NewBuilder(artifacts.BuilderOptions{})
	if err := r.addStage(name, &stage{publish: builder.Publish, close: builder.Close}); err != nil {
		_ = builder.Close()
		return nil, err
	}
	return &TreeStage{builder: builder}, nil
}

// Content stores bytes in CAS for use in a tree entry.
func (r *Run) Content(ctx context.Context, content io.Reader) (cas.PutResult, error) {
	return r.coordinator.options.CAS.Put(ctx, content)
}

// Existing stages an already registered artifact as an output.
func (r *Run) Existing(name string, id artifacts.ID) error {
	return r.addStage(name, &stage{publish: func(context.Context) (artifacts.ID, error) { return id, nil }})
}

// Keep marks a staged output to publish even if the run fails.
func (r *Run) Keep(name string) error {
	if _, ok := r.stages[name]; !ok {
		return fmt.Errorf("%w: keep of unknown output %q", ErrInvalid, name)
	}
	if r.kept == nil {
		r.kept = map[string]bool{}
	}
	r.kept[name] = true
	return nil
}

// PlanAssertion queues an assertion for finalization, signed with Job.Writer. If
// output is named the subject is that output's occurrence; otherwise request.Subject
// stands. An empty request.ID is derived from the completion record, so a retry
// converges on one assertion. It is recorded only on success.
func (r *Run) PlanAssertion(output string, request assertions.Request) error {
	if output != "" {
		if _, ok := r.stages[output]; !ok {
			return fmt.Errorf("%w: assertion about unknown output %q", ErrInvalid, output)
		}
	}
	r.planned = append(r.planned, plannedAssertion{ID: request.ID, Output: output, Kind: request.Kind, Subject: request.Subject, Value: request.Value, Evidence: request.Evidence, Key: request.Key, Replaces: request.Replaces})
	return nil
}

// checkRead enforces strictness and records the read. Exact declared references, and
// any path inside a declared snapshot, count as declared.
func (r *Run) checkRead(ref provenance.Ref) error {
	if r.declared[ref] {
		return nil
	}
	if ref.Path != "" {
		parent := provenance.Ref{Artifact: ref.Artifact, Occurrence: ref.Occurrence, Revision: ref.Revision}
		if r.declared[parent] {
			return nil
		}
	}
	if r.job.Strict {
		return fmt.Errorf("%w: %+v", ErrUndeclaredRead, ref)
	}
	if r.reads == nil {
		r.reads = map[provenance.Ref]bool{}
	}
	r.reads[ref] = true
	return nil
}

// Open reads a file of an artifact. Artifacts are immutable, so the read is as pinned
// as it can be; the check is whether it was declared.
func (r *Run) Open(ctx context.Context, artifact artifacts.ID, path string) (io.ReadCloser, error) {
	if err := r.checkRead(provenance.Ref{Artifact: string(artifact), Path: path}); err != nil {
		return nil, err
	}
	return r.coordinator.options.Artifacts.OpenFile(ctx, artifact, path)
}

// Head reads a guarded history's tip as of the run's start, so a writer advancing it
// mid-run does not change what this run sees. The revision read is recorded.
func (r *Run) Head(ctx context.Context, history string) (lineage.ID, error) {
	id, err := r.coordinator.options.Lineage.HeadAt(ctx, history, r.pins.Lineage)
	if err != nil {
		return "", err
	}
	if err := r.checkRead(provenance.Ref{Revision: string(id)}); err != nil {
		return "", err
	}
	return id, nil
}

// Assertions queries as of the run's start. Each returned assertion is a read and is
// held to the same declaration rule as any other input.
func (r *Run) Assertions(ctx context.Context, q assertions.Query) (assertions.Page, error) {
	if r.coordinator.options.Assertions == nil {
		return assertions.Page{}, fmt.Errorf("%w: no assertion service", ErrInvalid)
	}
	q.AsOf = r.pins.Assertions
	page, err := r.coordinator.options.Assertions.List(ctx, q)
	if err != nil {
		return page, err
	}
	if r.pins.Assertions == 0 {
		// An empty journal at the start pins to nothing, and zero means "now".
		return assertions.Page{}, nil
	}
	for _, a := range page.Assertions {
		if err := r.checkRead(provenance.Ref{Assertion: string(a.ID)}); err != nil {
			return assertions.Page{}, err
		}
	}
	return page, nil
}
