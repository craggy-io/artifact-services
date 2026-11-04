package runs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/adapters/runs"
	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
)

func code(name string) runs.Code { return runs.Code{Kind: "builtin", Name: name, Version: "1"} }

func writeOutput(data string) func(context.Context, *runs.Run) error {
	return func(ctx context.Context, r *runs.Run) error {
		return r.Blob(ctx, "out.txt", strings.NewReader(data), 0o644)
	}
}

func TestSuccessfulRunPublishesAndRecordsLineage(t *testing.T) {
	s := testkit.Open(t)
	session := s.Session("alice")
	input := s.File("in.txt", "input")
	result, err := session.Run(s.Ctx, runs.Job{ID: "r1", Code: code("copy"), Inputs: []provenance.Ref{{Artifact: string(input)}}, Params: json.RawMessage(`{"x":1}`)}, func(ctx context.Context, r *runs.Run) error {
		reader, err := r.Open(ctx, input, "in.txt")
		if err != nil {
			return err
		}
		defer reader.Close()
		return r.Blob(ctx, "out.txt", reader, 0o755)
	})
	if err != nil {
		t.Fatal(err)
	}
	output, ok := result.Output("out.txt")
	if !ok || result.Reused || output.Occurrence == "" {
		t.Fatalf("result = %+v", result)
	}
	occurrence, err := s.Artifacts.GetOccurrence(s.Ctx, output.Occurrence)
	if err != nil || occurrence.Name != "out.txt" || occurrence.Producer != string(result.Completion) {
		t.Fatalf("occurrence = %+v err = %v", occurrence, err)
	}
	info, err := s.Artifacts.Stat(s.Ctx, output.Artifact, "out.txt")
	if err != nil || info.Mode != 0o755 {
		t.Fatalf("info = %+v err = %v", info, err)
	}
	edges, err := s.Lineage.List(s.Ctx, lineage.Query{RelationshipKind: runs.KindDerivedFrom, Ref: &provenance.Ref{Occurrence: string(output.Occurrence)}})
	if err != nil || len(edges.Records) != 1 || edges.Records[0].Relationship.To.Artifact != string(input) {
		t.Fatalf("edges = %+v err = %v", edges, err)
	}
	completion, err := s.Lineage.Get(s.Ctx, result.Completion)
	if err != nil || completion.Activity.Actor != "alice" || len(completion.Activity.Outputs) != 1 {
		t.Fatalf("completion = %+v err = %v", completion, err)
	}
	if err := session.End(s.Ctx, nil); err != nil {
		t.Fatal(err)
	}
	unfinished, err := s.Runs().Unfinished(s.Ctx)
	if err != nil || len(unfinished) != 0 {
		t.Fatalf("unfinished = %+v err = %v", unfinished, err)
	}
}

func TestFailurePublishesOnlyKeptOutputs(t *testing.T) {
	s := testkit.Open(t)
	session := s.Session("alice")
	boom := errors.New("boom")
	result, err := session.Run(s.Ctx, runs.Job{ID: "r1", Code: code("job")}, func(ctx context.Context, r *runs.Run) error {
		if err := r.Blob(ctx, "kept", strings.NewReader("evidence"), 0); err != nil {
			return err
		}
		if err := r.Blob(ctx, "dropped", strings.NewReader("never visible"), 0); err != nil {
			return err
		}
		if err := r.Keep("kept"); err != nil {
			return err
		}
		return boom
	})
	var failed *runs.RunError
	if !errors.Is(err, boom) || !errors.As(err, &failed) {
		t.Fatalf("err = %v", err)
	}
	if len(result.Outputs) != 1 || result.Outputs[0].Name != "kept" || len(failed.Outputs) != 1 {
		t.Fatalf("result = %+v failed = %+v", result, failed)
	}
	if _, err := s.Artifacts.GetOccurrence(s.Ctx, result.Outputs[0].Occurrence); err != nil {
		t.Fatal(err)
	}
	page, err := s.Artifacts.Occurrences(s.Ctx, artifacts.OccurrenceQuery{Name: "dropped"})
	if err != nil || len(page.Occurrences) != 0 {
		t.Fatalf("dropped output visible: %+v err = %v", page, err)
	}
	found := map[string]bool{}
	search, err := s.Artifacts.Search(s.Ctx, artifacts.CatalogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range search.Items {
		found[string(item.ID)] = true
	}
	if len(found) != 1 {
		t.Fatalf("a dropped output reached the catalog: %+v", search.Items)
	}
	// The failure is immutable: the same ID reports it rather than re-running.
	ran := false
	_, err = session.Run(s.Ctx, runs.Job{ID: "r1", Code: code("job")}, func(context.Context, *runs.Run) error { ran = true; return nil })
	if !errors.Is(err, runs.ErrPriorFailure) || ran {
		t.Fatalf("rerun of failure: err = %v ran = %t", err, ran)
	}
}

func TestStableIDMakesARunIdempotent(t *testing.T) {
	s := testkit.Open(t)
	session := s.Session("alice")
	calls := 0
	job := runs.Job{ID: "stable", Code: code("job")}
	fn := func(ctx context.Context, r *runs.Run) error {
		calls++
		return r.Blob(ctx, "o", strings.NewReader("x"), 0)
	}
	first, err := session.Run(s.Ctx, job, fn)
	if err != nil {
		t.Fatal(err)
	}
	second, err := session.Run(s.Ctx, job, fn)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !second.Reused || second.Outputs[0] != first.Outputs[0] {
		t.Fatalf("calls = %d first = %+v second = %+v", calls, first, second)
	}
}

func TestPinnedAndStrictReads(t *testing.T) {
	s := testkit.Open(t)
	session := s.Session("alice")
	declared := s.File("a", "A")
	other := s.File("b", "B")
	if _, err := s.Lineage.Append(s.Ctx, lineage.AppendRevision{ID: "h1", History: "h", Artifact: declared}); err != nil {
		t.Fatal(err)
	}
	kind, err := s.Assertions.Register(s.Ctx, assertions.Kind{Name: "note", Version: 1, Shape: objects.Shape{Type: "integer"}}, assertions.Writer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Assertions.Record(s.Ctx, assertions.Request{ID: "before", Kind: kind, Subject: provenance.Ref{Artifact: string(declared)}, Value: json.RawMessage("1")}); err != nil {
		t.Fatal(err)
	}
	_, err = session.Run(s.Ctx, runs.Job{ID: "strict", Code: code("job"), Strict: true, Inputs: []provenance.Ref{{Artifact: string(declared)}, {Revision: "h1"}, {Assertion: "before"}}}, func(ctx context.Context, r *runs.Run) error {
		if reader, err := r.Open(ctx, declared, "a"); err != nil {
			t.Errorf("declared read = %v", err)
		} else {
			reader.Close()
		}
		if _, err := r.Open(ctx, other, "b"); !errors.Is(err, runs.ErrUndeclaredRead) {
			t.Errorf("undeclared read = %v", err)
		}
		// Advance state mid-run: the run must keep seeing its starting point.
		if _, err := s.Lineage.Append(ctx, lineage.AppendRevision{ID: "h2", History: "h", Expected: "h1", Artifact: declared}); err != nil {
			t.Error(err)
		}
		if _, err := s.Assertions.Record(ctx, assertions.Request{ID: "after", Kind: kind, Subject: provenance.Ref{Artifact: string(declared)}, Value: json.RawMessage("2")}); err != nil {
			t.Error(err)
		}
		head, err := r.Head(ctx, "h")
		if err != nil || head != "h1" {
			t.Errorf("pinned head = %s err = %v", head, err)
		}
		page, err := r.Assertions(ctx, assertions.Query{Kind: kind})
		if err != nil || len(page.Assertions) != 1 || page.Assertions[0].ID != "before" {
			t.Errorf("pinned assertions = %+v err = %v", page, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if head, _ := s.Lineage.Head(s.Ctx, "h"); head != "h2" {
		t.Fatalf("outside the run head = %s", head)
	}
	// Non-strict: an undeclared read is allowed, and recorded as an input.
	result, err := session.Run(s.Ctx, runs.Job{ID: "loose", Code: code("job")}, func(ctx context.Context, r *runs.Run) error {
		reader, err := r.Open(ctx, other, "b")
		if err != nil {
			return err
		}
		defer reader.Close()
		if _, err := io.Copy(io.Discard, reader); err != nil {
			return err
		}
		return r.Blob(ctx, "o", strings.NewReader("derived"), 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	completion, err := s.Lineage.Get(s.Ctx, result.Completion)
	if err != nil || len(completion.Activity.Inputs) != 1 || completion.Activity.Inputs[0].Artifact != string(other) || completion.Activity.Inputs[0].Path != "b" {
		t.Fatalf("recorded inputs = %+v err = %v", completion.Activity.Inputs, err)
	}
}

func TestDeterministicReuseAcrossCalls(t *testing.T) {
	s := testkit.Open(t)
	session := s.Session("alice")
	input := s.File("in", "payload")
	calls := 0
	job := func(id string) runs.Job {
		return runs.Job{ID: id, Code: code("hash"), Deterministic: true, Inputs: []provenance.Ref{{Artifact: string(input)}}, Params: json.RawMessage(`{"a":1,"b":2}`)}
	}
	fn := func(ctx context.Context, r *runs.Run) error {
		calls++
		return writeOutput("digest")(ctx, r)
	}
	first, err := session.Run(s.Ctx, job("one"), fn)
	if err != nil {
		t.Fatal(err)
	}
	if first.Reused || calls != 1 {
		t.Fatalf("first = %+v calls = %d", first, calls)
	}
	second, err := session.Run(s.Ctx, job("two"), fn)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Reused || calls != 1 || second.Outputs[0] != first.Outputs[0] || second.Run != "one" {
		t.Fatalf("second = %+v calls = %d", second, calls)
	}
	// Param key order does not matter: the key is canonical.
	reordered := job("three")
	reordered.Params = json.RawMessage(`{"b":2,"a":1}`)
	if third, err := session.Run(s.Ctx, reordered, fn); err != nil || !third.Reused {
		t.Fatalf("reordered = %+v err = %v", third, err)
	}
	// Any change to code, params or input content is a different derivation.
	for name, j := range map[string]runs.Job{
		"params": {ID: "p", Code: code("hash"), Deterministic: true, Inputs: job("").Inputs, Params: json.RawMessage(`{"a":2}`)},
		"code":   {ID: "c", Code: code("other"), Deterministic: true, Inputs: job("").Inputs, Params: job("").Params},
		"input":  {ID: "i", Code: code("hash"), Deterministic: true, Inputs: []provenance.Ref{{Artifact: string(s.File("in", "changed"))}}, Params: job("").Params},
	} {
		before := calls
		if result, err := session.Run(s.Ctx, j, fn); err != nil || result.Reused || calls != before+1 {
			t.Errorf("%s reused a run: %+v err = %v", name, result, err)
		}
	}
	// A deterministic failure is not memoized.
	failing := runs.Job{ID: "f1", Code: code("flaky"), Deterministic: true}
	if _, err := session.Run(s.Ctx, failing, func(context.Context, *runs.Run) error { return errors.New("transient") }); err == nil {
		t.Fatal("failure not reported")
	}
	failing.ID = "f2"
	if result, err := session.Run(s.Ctx, failing, writeOutput("fine")); err != nil || result.Reused {
		t.Fatalf("after failure = %+v err = %v", result, err)
	}
}

func TestDerivationKeyFollowsContentNotLocation(t *testing.T) {
	s := testkit.Open(t)
	c := s.Runs()
	input := s.File("in", "payload")
	if _, err := s.Artifacts.RecordOccurrence(s.Ctx, artifacts.OccurrenceRequest{ID: "o1", Artifact: input, Name: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Artifacts.RecordOccurrence(s.Ctx, artifacts.OccurrenceRequest{ID: "o2", Artifact: input, Name: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lineage.Append(s.Ctx, lineage.AppendRevision{ID: "rev", History: "h", Artifact: input}); err != nil {
		t.Fatal(err)
	}
	key := func(ref provenance.Ref) string {
		t.Helper()
		k, err := c.DerivationKey(s.Ctx, runs.Job{Code: code("x"), Inputs: []provenance.Ref{ref}})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	base := key(provenance.Ref{Artifact: string(input)})
	for _, ref := range []provenance.Ref{{Occurrence: "o1"}, {Occurrence: "o2"}, {Revision: "rev"}} {
		if got := key(ref); got != base {
			t.Errorf("%+v derives %s, want %s", ref, got, base)
		}
	}
	info, err := s.Artifacts.Stat(s.Ctx, input, "in")
	if err != nil {
		t.Fatal(err)
	}
	if key(provenance.Ref{Artifact: string(input), Path: "in"}) != key(provenance.Ref{Digest: string(info.Content)}) {
		t.Error("a path and its content digest derive differently")
	}
}

// TestDeterministicRaceHasOneResult is qualification scenario 6: separate
// coordinators share services, so in-process suppression cannot hide the race.
func TestDeterministicRaceHasOneResult(t *testing.T) {
	s := testkit.Open(t)
	input := s.File("in", "payload")
	const racers = 8
	var executed, reused atomic.Int64
	release := make(chan struct{})
	results := make([]runs.Result, racers)
	errs := make([]error, racers)
	var entered sync.WaitGroup
	entered.Add(racers)
	var wg sync.WaitGroup
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, err := s.Runs().Begin(s.Ctx, runs.SessionRequest{Actor: fmt.Sprintf("worker-%d", i)})
			if err != nil {
				errs[i] = err
				entered.Done()
				return
			}
			var once sync.Once
			results[i], errs[i] = session.Run(s.Ctx, runs.Job{ID: fmt.Sprintf("race-%d", i), Code: code("c"), Deterministic: true, Inputs: []provenance.Ref{{Artifact: string(input)}}}, func(ctx context.Context, r *runs.Run) error {
				executed.Add(1)
				once.Do(entered.Done)
				<-release
				// Each execution produces different bytes, so a merged result would show.
				return r.Blob(ctx, "o", strings.NewReader(fmt.Sprintf("from-%d", i)), 0)
			})
			if results[i].Reused {
				reused.Add(1)
			}
		}()
	}
	entered.Wait()
	close(release)
	wg.Wait()
	winners := map[artifacts.OccurrenceID]bool{}
	for i := range racers {
		if errs[i] != nil {
			t.Fatalf("racer %d: %v", i, errs[i])
		}
		winners[results[i].Outputs[0].Occurrence] = true
	}
	if len(winners) != 1 || reused.Load() != racers-1 || executed.Load() != racers {
		t.Fatalf("winners = %v reused = %d executed = %d", winners, reused.Load(), executed.Load())
	}
	page, err := s.Artifacts.Occurrences(s.Ctx, artifacts.OccurrenceQuery{Name: "o"})
	if err != nil || len(page.Occurrences) != 1 {
		t.Fatalf("visible occurrences = %+v err = %v", page, err)
	}
	unfinished, err := s.Runs().Unfinished(s.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range unfinished {
		if u.Run {
			t.Errorf("run left unfinished after losing: %+v", u)
		}
	}
	// One coordinator suppresses identical in-flight work instead of racing it.
	var calls atomic.Int64
	c := s.Runs()
	session, err := c.Begin(s.Ctx, runs.SessionRequest{Actor: "x"})
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := session.Run(s.Ctx, runs.Job{ID: fmt.Sprintf("solo-%d", i), Code: code("solo"), Deterministic: true}, func(ctx context.Context, r *runs.Run) error {
				calls.Add(1)
				<-gate
				return r.Blob(ctx, "s", strings.NewReader("x"), 0)
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond) // Wait for the leader; the rest queue behind it.
	}
	close(gate)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("suppressed coordinator executed %d times", calls.Load())
	}
}

func TestPlannedAssertionsRecordOnSuccessOnly(t *testing.T) {
	authority := assertions.NewAuthority()
	s := testkit.Open(t, testkit.Options{Authority: authority, Relaxed: true})
	kind, err := s.Assertions.Register(s.Ctx, assertions.Kind{Name: "result", Version: 1, Shape: objects.Shape{Type: "integer"}, Writers: []string{"host"}}, assertions.Writer{})
	if err != nil {
		t.Fatal(err)
	}
	session := s.Session("alice")
	writer := authority.Credential("host")
	plan := func(ctx context.Context, r *runs.Run) error {
		if err := writeOutput("o")(ctx, r); err != nil {
			return err
		}
		return r.PlanAssertion("out.txt", assertions.Request{Kind: kind, Value: json.RawMessage("7")})
	}
	result, err := session.Run(s.Ctx, runs.Job{ID: "ok", Code: code("a"), Writer: writer}, plan)
	if err != nil {
		t.Fatal(err)
	}
	subject := provenance.Ref{Occurrence: string(result.Outputs[0].Occurrence)}
	page, err := s.Assertions.List(s.Ctx, assertions.Query{Subject: &subject})
	if err != nil || len(page.Assertions) != 1 || page.Assertions[0].Writer != "host" || page.Assertions[0].Activity != string(result.Completion) {
		t.Fatalf("assertions = %+v err = %v", page, err)
	}
	// Re-finalizing converges on the same assertion.
	again, err := session.Run(s.Ctx, runs.Job{ID: "ok", Code: code("a"), Writer: writer}, plan)
	if err != nil || !again.Reused {
		t.Fatalf("again = %+v err = %v", again, err)
	}
	if page, _ = s.Assertions.List(s.Ctx, assertions.Query{Subject: &subject, IncludeEnded: true}); len(page.Assertions) != 1 {
		t.Fatalf("duplicated: %+v", page)
	}
	// The run's authority is the job's credential; it cannot launder a missing one.
	_, err = session.Run(s.Ctx, runs.Job{ID: "unsigned", Code: code("b")}, func(ctx context.Context, r *runs.Run) error {
		if err := r.Blob(ctx, "o2", strings.NewReader("x"), 0); err != nil {
			return err
		}
		return r.PlanAssertion("o2", assertions.Request{Kind: kind, Value: json.RawMessage("1")})
	})
	if !errors.Is(err, assertions.ErrWriterForbidden) {
		t.Fatalf("unsigned plan = %v", err)
	}
	// A failed run records no planned assertion.
	before, _ := s.Assertions.Sequence(s.Ctx)
	_, err = session.Run(s.Ctx, runs.Job{ID: "bad", Code: code("c"), Writer: writer}, func(ctx context.Context, r *runs.Run) error {
		if err := r.Blob(ctx, "o3", strings.NewReader("y"), 0); err != nil {
			return err
		}
		if err := r.PlanAssertion("o3", assertions.Request{Kind: kind, Value: json.RawMessage("1")}); err != nil {
			return err
		}
		return errors.New("failed")
	})
	if err == nil {
		t.Fatal("failure not reported")
	}
	if after, _ := s.Assertions.Sequence(s.Ctx); after != before {
		t.Fatalf("failed run recorded assertions: %d to %d", before, after)
	}
}

func TestSessionsNestInheritAndReportUnfinished(t *testing.T) {
	s := testkit.Open(t)
	c := s.Runs()
	parent, err := c.Begin(s.Ctx, runs.SessionRequest{ID: "parent", Actor: "alice", Owner: "team"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.Child(s.Ctx, runs.SessionRequest{ID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if child.Actor() != "alice" || child.Owner() != "team" {
		t.Fatalf("child = %s/%s", child.Actor(), child.Owner())
	}
	record, err := s.Lineage.Get(s.Ctx, "session/child/start")
	if err != nil || string(record.Activity.Metadata["parent"]) != `"parent"` {
		t.Fatalf("child start = %+v err = %v", record, err)
	}
	if err := child.End(s.Ctx, errors.New("no")); err != nil {
		t.Fatal(err)
	}
	if err := child.End(s.Ctx, errors.New("no")); err != nil {
		t.Fatalf("identical end = %v", err)
	}
	if err := child.End(s.Ctx, nil); !errors.Is(err, lineage.ErrConflict) {
		t.Fatalf("contradicting end = %v", err)
	}
	s.Reopen()
	unfinished, err := s.Runs().Unfinished(s.Ctx)
	if err != nil || len(unfinished) != 1 || unfinished[0].ID != "parent" || unfinished[0].Run {
		t.Fatalf("unfinished = %+v err = %v", unfinished, err)
	}
}

func TestTreeOutputsStayPrivateUntilSuccess(t *testing.T) {
	s := testkit.Open(t)
	session := s.Session("alice")
	before, err := s.Artifacts.Sequence(s.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Run(s.Ctx, runs.Job{ID: "tree", Code: code("t")}, func(ctx context.Context, r *runs.Run) error {
		tree, err := r.Tree("t")
		if err != nil {
			return err
		}
		for i := range 3 {
			put, err := r.Content(ctx, strings.NewReader(fmt.Sprintf("file %d", i)))
			if err != nil {
				return err
			}
			if err := tree.Add(artifacts.Entry{Path: fmt.Sprintf("d/%d", i), Content: put.ID, Size: put.Size}); err != nil {
				return err
			}
		}
		if now, _ := s.Artifacts.Sequence(ctx); now != before {
			t.Errorf("staged tree reached the catalog mid-run: %d to %d", before, now)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.Artifacts.ListFiles(s.Ctx, result.Outputs[0].Artifact, artifacts.FileQuery{})
	if err != nil || len(page.Entries) != 3 {
		t.Fatalf("files = %+v err = %v", page, err)
	}
}
