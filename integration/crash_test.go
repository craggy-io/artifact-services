package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/adapters/gc"
	"github.com/craggy-io/artifact-services/adapters/runs"
	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/internal/crashpoint"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
)

const (
	crashRootEnv = "ARTIFACT_SERVICES_CRASH_ROOT"
	crashHoldEnv = "ARTIFACT_SERVICES_CRASH_HOLD"
	ackFile      = "acknowledged.txt"
	doneFile     = "done.txt"
	readyFile    = "ready.txt"
)

// compositeWorkload is the composite operation consumers perform, with every ID
// stable: publish, record an occurrence, record lineage through a run, deterministic
// reuse, assertions, guarded history. It is the identical call sequence every time,
// so the only thing that varies is where a previous attempt died.
func compositeWorkload(ctx context.Context, root string, ack func(string)) (result error) {
	authority := assertions.NewAuthority()
	st, err := stack.Open(stack.Options{Root: root, Authority: authority})
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { result = errors.Join(result, st.Close()) }()
	writer := authority.Credential("host")

	kind, err := st.Assertions.Register(ctx, assertions.Kind{Name: "qualified", Version: 1, Shape: objects.Shape{Type: "integer"}, Keyed: true, Writers: []string{"host"}}, assertions.Writer{})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	ack("kind " + string(kind))

	put, err := st.CAS.Put(ctx, strings.NewReader("composite input"))
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	input, err := st.Artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "input.txt", Content: put.ID, Size: put.Size, Mode: 0o644}}})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	ack("artifact " + string(input))
	occurrence, err := st.Artifacts.RecordOccurrence(ctx, artifacts.OccurrenceRequest{ID: "input-occurrence", Artifact: input, Name: "input.txt"})
	if err != nil {
		return fmt.Errorf("occurrence: %w", err)
	}
	ack("occurrence " + string(occurrence.ID))

	coordinator, err := runs.New(runs.Options{CAS: st.CAS, Artifacts: st.Artifacts, Lineage: st.Lineage, Assertions: st.Assertions})
	if err != nil {
		return err
	}
	session, err := coordinator.Begin(ctx, runs.SessionRequest{ID: "session", Actor: "actor"})
	if err != nil {
		return fmt.Errorf("session: %w", err)
	}
	ack("record " + "session/session/start")

	inputs := []provenance.Ref{{Occurrence: string(occurrence.ID)}}
	plain, err := session.Run(ctx, runs.Job{ID: "plain", Code: runs.Code{Kind: "builtin", Name: "plain"}, Inputs: inputs, Writer: writer}, func(ctx context.Context, r *runs.Run) error {
		if err := r.Blob(ctx, "plain.txt", strings.NewReader("plain output"), 0o644); err != nil {
			return err
		}
		return r.PlanAssertion("plain.txt", assertions.Request{Kind: kind, Key: "plain", Value: json.RawMessage("1")})
	})
	if err != nil {
		return fmt.Errorf("plain run: %w", err)
	}
	ack("record " + string(plain.Completion))

	deterministic, err := session.Run(ctx, runs.Job{ID: "derived", Code: runs.Code{Kind: "builtin", Name: "derive"}, Inputs: inputs, Params: json.RawMessage(`{"n":1}`), Deterministic: true, Writer: writer}, func(ctx context.Context, r *runs.Run) error {
		tree, err := r.Tree("tree")
		if err != nil {
			return err
		}
		content, err := r.Content(ctx, strings.NewReader("tree output"))
		if err != nil {
			return err
		}
		if err := tree.Add(artifacts.Entry{Path: "dir/file", Content: content.ID, Size: content.Size, Mode: 0o755}); err != nil {
			return err
		}
		return tree.Add(artifacts.Entry{Path: "empty", Kind: artifacts.KindDir})
	})
	if err != nil {
		return fmt.Errorf("deterministic run: %w", err)
	}
	ack("record " + string(deterministic.Completion))

	subject := provenance.Ref{Occurrence: string(occurrence.ID)}
	for _, step := range []assertions.Request{
		{ID: "first", Kind: kind, Key: "k", Subject: subject, Value: json.RawMessage("1"), Writer: writer},
		{ID: "second", Kind: kind, Key: "k", Subject: subject, Value: json.RawMessage("2"), Replaces: "first", Writer: writer},
		{ID: "gone", Kind: kind, Key: "gone", Subject: subject, Value: json.RawMessage("3"), Writer: writer},
	} {
		if _, err := st.Assertions.Record(ctx, step); err != nil {
			return fmt.Errorf("assert %s: %w", step.ID, err)
		}
		ack("assertion " + string(step.ID))
	}
	if err := st.Assertions.Retract(ctx, assertions.Retraction{Assertion: "gone", Writer: writer}); err != nil {
		return fmt.Errorf("retract: %w", err)
	}
	ack("retracted gone")

	if _, err := st.Lineage.Append(ctx, lineage.AppendRevision{ID: "v1", History: "doc", Artifact: input}); err != nil {
		return fmt.Errorf("append v1: %w", err)
	}
	ack("record v1")
	output, _ := plain.Output("plain.txt")
	if _, err := st.Lineage.Append(ctx, lineage.AppendRevision{ID: "v2", History: "doc", Expected: "v1", Artifact: output.Artifact}); err != nil {
		return fmt.Errorf("append v2: %w", err)
	}
	ack("record v2")

	if err := session.End(ctx, nil); err != nil {
		return fmt.Errorf("end: %w", err)
	}
	ack("record session/session/end")
	return nil
}

// TestCrashWorkloadHelper is not a test: it is the victim process. See runVictim.
func TestCrashWorkloadHelper(t *testing.T) {
	root := os.Getenv(crashRootEnv)
	if root == "" {
		t.Skip("helper for TestKillAtEveryWriteStageBoundary")
	}
	if os.Getenv(crashHoldEnv) != "" {
		holdStackOpen(t, root)
		return
	}
	if target := os.Getenv(crashpoint.EnvVar); target != "" {
		name, nth, ok := strings.Cut(target, "#")
		count, err := strconv.ParseInt(nth, 10, 64)
		if !ok || err != nil {
			t.Fatalf("crash target %q", target)
		}
		crashpoint.KillOnHit(name, count)
	}
	acknowledged, err := os.OpenFile(filepath.Join(root, ackFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer acknowledged.Close()
	ack := func(line string) {
		if _, err := acknowledged.WriteString(line + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := compositeWorkload(context.Background(), filepath.Join(root, "stack"), ack); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, doneFile), nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func holdStackOpen(t *testing.T, root string) {
	st, err := stack.Open(stack.Options{Root: filepath.Join(root, "stack")})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := os.WriteFile(filepath.Join(root, readyFile), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Hour)
}

func victimCommand(root string, env ...string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestCrashWorkloadHelper$", "-test.count=1")
	command.Env = append(append(os.Environ(), crashRootEnv+"="+root), env...)
	return command
}

// runVictim runs the workload in another process and dies at the target. The test
// binary survives nothing about the victim except the files it left.
func runVictim(t *testing.T, root, target string) {
	t.Helper()
	output, err := victimCommand(root, crashpoint.EnvVar+"="+target).CombinedOutput()
	if _, statErr := os.Stat(filepath.Join(root, doneFile)); statErr == nil {
		t.Fatalf("victim finished instead of dying at %s:\n%s", target, output)
	}
	if err == nil {
		t.Fatalf("victim exited cleanly at %s:\n%s", target, output)
	}
	if strings.Contains(string(output), "--- FAIL") {
		t.Fatalf("victim failed rather than died at %s:\n%s", target, output)
	}
}

// compositeState is everything visible, without the sequences that a retry's commit
// order legitimately changes.
type compositeState struct {
	Artifacts   []string
	Occurrences []string
	Records     []string
	Assertions  []string
	Kinds       []string
	Heads       map[string]string
	Unfinished  []string
}

func eachChange(ctx context.Context, read func(context.Context, provenance.ChangeQuery) (provenance.ChangePage, error), visit func(provenance.Change)) (uint64, error) {
	position := uint64(0)
	for {
		page, err := read(ctx, provenance.ChangeQuery{After: position, Limit: 1000, IncludeInternal: true})
		if err != nil {
			return position, err
		}
		for _, change := range page.Changes {
			if change.Sequence != position+1 {
				return position, fmt.Errorf("sequence gap: %d follows %d", change.Sequence, position)
			}
			position = change.Sequence
			visit(change)
		}
		if len(page.Changes) < 1000 {
			return position, nil
		}
	}
}

func captureState(ctx context.Context, st *stack.Stack) (compositeState, error) {
	var state compositeState
	if _, err := eachChange(ctx, st.Artifacts.Changes, func(c provenance.Change) {
		if c.Class == artifacts.ChangeOccurrence {
			state.Occurrences = append(state.Occurrences, c.ID)
		} else {
			state.Artifacts = append(state.Artifacts, c.ID)
		}
	}); err != nil {
		return state, fmt.Errorf("artifact feed: %w", err)
	}
	if _, err := eachChange(ctx, st.Lineage.Changes, func(c provenance.Change) { state.Records = append(state.Records, c.ID) }); err != nil {
		return state, fmt.Errorf("lineage feed: %w", err)
	}
	var assertionIDs []string
	if _, err := eachChange(ctx, st.Assertions.Changes, func(c provenance.Change) {
		switch c.Class {
		case assertions.ChangeKind:
			state.Kinds = append(state.Kinds, c.ID)
		case assertions.ChangeAssertion:
			assertionIDs = append(assertionIDs, c.ID)
		}
	}); err != nil {
		return state, fmt.Errorf("assertion feed: %w", err)
	}
	for _, id := range assertionIDs {
		a, err := st.Assertions.Get(ctx, assertions.ID(id))
		if err != nil {
			return state, err
		}
		state.Assertions = append(state.Assertions, fmt.Sprintf("%s kind=%s key=%s value=%s writer=%s current=%t ended_by=%s retracted=%t", a.ID, a.Kind, a.Key, a.Value, a.Writer, a.Current, a.EndedBy, a.Retracted))
	}
	if head, err := st.Lineage.Head(ctx, "doc"); err == nil {
		state.Heads = map[string]string{"doc": string(head)}
	} else if !errors.Is(err, lineage.ErrNotFound) {
		return state, err
	}
	coordinator, err := runs.New(runs.Options{CAS: st.CAS, Artifacts: st.Artifacts, Lineage: st.Lineage, Assertions: st.Assertions})
	if err != nil {
		return state, err
	}
	unfinished, err := coordinator.Unfinished(ctx)
	if err != nil {
		return state, err
	}
	for _, u := range unfinished {
		state.Unfinished = append(state.Unfinished, u.ID)
	}
	for _, list := range [][]string{state.Artifacts, state.Occurrences, state.Records, state.Assertions, state.Kinds, state.Unfinished} {
		sort.Strings(list)
	}
	return state, nil
}

func services(st *stack.Stack) gc.Services {
	return gc.Services{CAS: st.CAS, Blobs: st.Blobs, Artifacts: st.Artifacts, Lineage: st.Lineage, Assertions: st.Assertions}
}

func verifyClean(ctx context.Context, st *stack.Stack) error {
	check, err := gc.Verify(ctx, services(st))
	if err != nil {
		return err
	}
	if !check.OK() {
		return fmt.Errorf("verify findings: %v", check.Findings)
	}
	return nil
}

// checkAcknowledged proves every commit reported successful survived the kill.
func checkAcknowledged(ctx context.Context, st *stack.Stack, root string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(root, ackFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	lines := strings.Fields(strings.ReplaceAll(string(data), "\n", " "))
	var acknowledged []string
	for i := 0; i+1 < len(lines); i += 2 {
		class, id := lines[i], lines[i+1]
		acknowledged = append(acknowledged, class+" "+id)
		switch class {
		case "kind":
			_, err = st.Assertions.GetKind(ctx, assertions.KindID(id))
		case "artifact":
			_, err = st.Artifacts.Category(ctx, artifacts.ID(id))
		case "occurrence":
			_, err = st.Artifacts.GetOccurrence(ctx, artifacts.OccurrenceID(id))
		case "record":
			_, err = st.Lineage.Get(ctx, lineage.ID(id))
		case "assertion":
			_, err = st.Assertions.Get(ctx, assertions.ID(id))
		case "retracted":
			var a assertions.Assertion
			if a, err = st.Assertions.Get(ctx, assertions.ID(id)); err == nil && !a.Retracted {
				err = errors.New("retraction was acknowledged but is absent")
			}
		default:
			err = fmt.Errorf("unknown class %q", class)
		}
		if err != nil {
			return nil, fmt.Errorf("acknowledged %s %s lost: %w", class, id, err)
		}
	}
	return acknowledged, nil
}

func survive(ctx context.Context, root string, baseline compositeState) (result error) {
	st, err := stack.Open(stack.Options{Root: filepath.Join(root, "stack")})
	if err != nil {
		return fmt.Errorf("reopen without surgery: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, st.Close())
		}
	}()
	acknowledged, err := checkAcknowledged(ctx, st, root)
	if err != nil {
		return err
	}
	if err := verifyClean(ctx, st); err != nil {
		return fmt.Errorf("after crash: %w", err)
	}
	state, err := captureState(ctx, st)
	if err != nil {
		return fmt.Errorf("after crash: %w", err)
	}
	// Crashed work stays visibly unfinished.
	// The ack is written after the commit, so absence of an end ack is not absence of
	// the end: a kill can fall between the two. The durable record decides.
	started := slices.Contains(acknowledged, "record session/session/start")
	ended := slices.Contains(state.Records, "session/session/end")
	if started && !ended && !slices.Contains(state.Unfinished, "session") {
		return fmt.Errorf("a crashed session is not visibly unfinished: %v", state.Unfinished)
	}
	closed = true
	if err := st.Close(); err != nil {
		return err
	}

	// Rerunning the identical composite converges on exactly the clean run's result.
	if err := compositeWorkload(ctx, filepath.Join(root, "stack"), func(string) {}); err != nil {
		return fmt.Errorf("rerun: %w", err)
	}
	final, err := stack.Open(stack.Options{Root: filepath.Join(root, "stack")})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, final.Close()) }()
	if err := verifyClean(ctx, final); err != nil {
		return fmt.Errorf("after rerun: %w", err)
	}
	converged, err := captureState(ctx, final)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(converged, baseline) {
		return fmt.Errorf("did not converge:\n got  %+v\n want %+v", converged, baseline)
	}
	return nil
}

// TestKillAtEveryWriteStageBoundary is A9(c), A14 and qualification scenario 5: a real
// process is killed at every hit of every boundary, without cleanup, then the store must reopen
// without file surgery, have kept everything it acknowledged, verify clean, show
// crashed work as unfinished, and converge when the composite is simply run again.
func TestKillAtEveryWriteStageBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns many processes")
	}
	ctx := context.Background()
	// A clean run gives the expected final state and how often each boundary is hit.
	var mu sync.Mutex
	hits := map[string]int64{}
	restore := crashpoint.Set(func(name string) {
		mu.Lock()
		defer mu.Unlock()
		hits[name]++
	})
	cleanRoot := t.TempDir()
	if err := compositeWorkload(ctx, filepath.Join(cleanRoot, "stack"), func(string) {}); err != nil {
		t.Fatal(err)
	}
	restore()
	clean, err := stack.Open(stack.Options{Root: filepath.Join(cleanRoot, "stack")})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := captureState(ctx, clean)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(verifyClean(ctx, clean), clean.Close()); err != nil {
		t.Fatal(err)
	}
	if len(baseline.Unfinished) != 0 || len(baseline.Records) == 0 || len(baseline.Occurrences) < 3 {
		t.Fatalf("baseline = %+v", baseline)
	}
	// Idempotence of the composite itself: a second run changes nothing visible.
	if err := compositeWorkload(ctx, filepath.Join(cleanRoot, "stack"), func(string) {}); err != nil {
		t.Fatal(err)
	}
	clean, err = stack.Open(stack.Options{Root: filepath.Join(cleanRoot, "stack")})
	if err != nil {
		t.Fatal(err)
	}
	again, err := captureState(ctx, clean)
	if err != nil {
		t.Fatal(err)
	}
	if err := clean.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again, baseline) {
		t.Fatalf("rerun of a finished composite changed state:\n got  %+v\n want %+v", again, baseline)
	}

	for _, boundary := range crashpoint.All {
		if hits[boundary] == 0 {
			t.Errorf("the workload never reaches boundary %s", boundary)
			continue
		}
		for nth := int64(1); nth <= hits[boundary]; nth++ {
			t.Run(fmt.Sprintf("%s#%d", boundary, nth), func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				runVictim(t, root, fmt.Sprintf("%s#%d", boundary, nth))
				if err := survive(ctx, root, baseline); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestLiveOwnerIsExcludedAndDeadOwnerIsNot is the other half of A9(b): the same root
// that opens freely after a kill stays closed while its owner lives.
func TestLiveOwnerIsExcludedAndDeadOwnerIsNot(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a process")
	}
	root := t.TempDir()
	holder := victimCommand(root, crashHoldEnv+"=1")
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = holder.Process.Kill()
			_ = holder.Wait()
		}
	}()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, readyFile)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("holder never became ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, err := stack.Open(stack.Options{Root: filepath.Join(root, "stack")})
	if !errors.Is(err, artifacts.ErrLeaseHeld) {
		t.Fatalf("open beside a live owner = %v", err)
	}
	var lease *artifacts.LeaseError
	if !errors.As(err, &lease) || lease.Owner.PID != holder.Process.Pid {
		t.Fatalf("lease = %+v, want owner %d", lease, holder.Process.Pid)
	}
	if err := holder.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.Wait()
	waited = true
	st, err := stack.Open(stack.Options{Root: filepath.Join(root, "stack")})
	if err != nil {
		t.Fatalf("open after the owner died = %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}
