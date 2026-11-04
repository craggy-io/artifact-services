// Package gc accounts for stored content no snapshot refers to, reclaims it behind a
// fence, and verifies that a stack's layers agree. Deciding what may become
// unreferenced is the consumer's policy: nothing here removes a registered artifact.
//
// # Why a sweep is safe
//
// Content can be stored long before anything refers to it, so "unreachable now" is not
// enough. A sweep therefore requires both of two conditions, and neither depends on
// journal sequences, which storage does not and should not know about:
//
//   - artifacts.Fence excludes every publication's check-then-register while
//     reachability is computed and blobs are removed. A publication that already
//     checked its content has registered by then, so it counts as reachable; one that
//     has not checked yet fails its check and reports missing content, never a
//     dangling artifact.
//   - The horizon spares anything written or re-Put recently, because storage refreshes
//     a blob's time when it is Put again. That protects work between Put and Publish,
//     so the horizon must exceed the longest such gap. Too short a horizon costs a
//     failed publication, which can be retried, never a dangling reference.
package gc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/provenance"
	"github.com/craggy-io/artifact-services/storage"
)

const digestPrefix = "sha256:"

// Orphan is stored content no registered artifact depends on.
type Orphan struct {
	Content cas.ID
	Size    int64
	ModTime time.Time
}

// Report describes stored content. Memory grows with the number of distinct reachable
// blobs, which it must hold to compare against what is stored.
type Report struct {
	Reachable   int64
	Stored      int64
	StoredBytes int64
	Orphans     []Orphan
	OrphanBytes int64
	// Missing is content a registered artifact needs that storage does not hold.
	Missing []cas.ID
}

func reachableSet(ctx context.Context, store *artifacts.Service) (map[cas.ID]bool, error) {
	reachable := map[cas.ID]bool{}
	err := store.Reachable(ctx, func(id cas.ID) error {
		reachable[id] = true
		return nil
	})
	return reachable, err
}

func account(ctx context.Context, blobs storage.Reclaimer, reachable map[cas.ID]bool) (Report, error) {
	report := Report{Reachable: int64(len(reachable))}
	present := map[cas.ID]bool{}
	err := blobs.Walk(ctx, func(key string, info storage.Info) error {
		id := cas.ID(digestPrefix + key)
		if _, err := cas.ParseID(string(id)); err != nil {
			return nil // Not CAS content, so not this tool's to judge.
		}
		present[id] = true
		report.Stored++
		report.StoredBytes += info.Size
		if !reachable[id] {
			report.Orphans = append(report.Orphans, Orphan{Content: id, Size: info.Size, ModTime: info.ModTime})
			report.OrphanBytes += info.Size
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	for id := range reachable {
		if !present[id] {
			report.Missing = append(report.Missing, id)
		}
	}
	return report, nil
}

// Scan reports orphans and missing content without changing anything. Taken without a
// fence, it may list as orphans content that a publication in flight is about to
// reference. Treat it as an estimate; only Sweep acts.
func Scan(ctx context.Context, blobs storage.Reclaimer, store *artifacts.Service) (Report, error) {
	reachable, err := reachableSet(ctx, store)
	if err != nil {
		return Report{}, err
	}
	return account(ctx, blobs, reachable)
}

// SweepOptions requires a horizon: content written or re-Put within it survives.
type SweepOptions struct {
	Horizon time.Duration
	// DryRun computes under the fence what would be removed, and removes nothing.
	DryRun bool
}

// SweepResult separates what was removed from what the horizon spared.
type SweepResult struct {
	Report
	Removed      []cas.ID
	RemovedBytes int64
	Spared       int64
	Pending      int
}

// ErrHorizon reports a sweep without a positive horizon.
var ErrHorizon = errors.New("gc: a positive horizon is required")

// Sweep removes unreachable content older than the horizon, plus temporary files from
// crashed writes. Publication blocks for its duration. See the package documentation
// for why it cannot remove content that is about to be referenced.
func Sweep(ctx context.Context, blobs storage.Reclaimer, store *artifacts.Service, options SweepOptions) (SweepResult, error) {
	if options.Horizon <= 0 {
		return SweepResult{}, ErrHorizon
	}
	var result SweepResult
	err := store.Fence(ctx, func(ctx context.Context) error {
		reachable, err := reachableSet(ctx, store)
		if err != nil {
			return err
		}
		if result.Report, err = account(ctx, blobs, reachable); err != nil {
			return err
		}
		cutoff := time.Now().Add(-options.Horizon)
		for _, orphan := range result.Orphans {
			if !orphan.ModTime.Before(cutoff) {
				result.Spared++
				continue
			}
			if options.DryRun {
				result.Removed = append(result.Removed, orphan.Content)
				result.RemovedBytes += orphan.Size
				continue
			}
			removed, err := blobs.Remove(ctx, string(orphan.Content)[len(digestPrefix):], cutoff)
			if err != nil {
				return err
			}
			if removed {
				result.Removed = append(result.Removed, orphan.Content)
				result.RemovedBytes += orphan.Size
			} else {
				result.Spared++
			}
		}
		if !options.DryRun {
			if result.Pending, err = blobs.SweepPending(ctx, cutoff); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

// Services are the layers Verify checks. Lineage and Assertions are optional.
type Services struct {
	CAS        cas.Store
	Blobs      storage.Reclaimer
	Artifacts  *artifacts.Service
	Lineage    *lineage.Service
	Assertions *assertions.Service
}

// Finding is one disagreement Verify found.
type Finding struct {
	Layer   string
	ID      string
	Problem string
}

func (f Finding) String() string { return fmt.Sprintf("%s %s: %s", f.Layer, f.ID, f.Problem) }

// Verification summarizes a verify pass. Findings are problems found; an error from
// Verify means the pass itself could not complete.
type Verification struct {
	ContentChecked int64
	BytesChecked   int64
	RecordsChecked int64
	Findings       []Finding
}

// OK reports a clean pass.
func (v Verification) OK() bool { return len(v.Findings) == 0 }

// Verify re-hashes all reachable content and checks that every record's references
// resolve across layers. It reads every byte, so its cost is the store's size.
func Verify(ctx context.Context, s Services) (Verification, error) {
	var out Verification
	finding := func(layer, id, format string, args ...any) {
		out.Findings = append(out.Findings, Finding{Layer: layer, ID: id, Problem: fmt.Sprintf(format, args...)})
	}
	reachable, err := reachableSet(ctx, s.Artifacts)
	if err != nil {
		// Reachability failing is itself the integrity finding: a manifest is unreadable.
		finding("artifacts", "", "reachability: %v", err)
	}
	for id := range reachable {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		size, err := rehash(ctx, s.CAS, id)
		out.ContentChecked++
		out.BytesChecked += size
		if err != nil {
			finding("cas", string(id), "%v", err)
		}
	}
	if err := eachChange(ctx, s.Artifacts.Changes, func(c provenance.Change) error {
		out.RecordsChecked++
		if c.Class != artifacts.ChangeOccurrence {
			return nil
		}
		occurrence, err := s.Artifacts.GetOccurrence(ctx, artifacts.OccurrenceID(c.ID))
		if err != nil {
			finding("artifacts", c.ID, "occurrence: %v", err)
			return nil
		}
		if _, err := s.Artifacts.Category(ctx, occurrence.Artifact); err != nil {
			finding("artifacts", c.ID, "occurrence target %s: %v", occurrence.Artifact, err)
		}
		return nil
	}); err != nil {
		return out, err
	}
	if s.Lineage != nil {
		if err := verifyLineage(ctx, s, finding, &out); err != nil {
			return out, err
		}
	}
	if s.Assertions != nil {
		if err := verifyAssertions(ctx, s, finding, &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

func rehash(ctx context.Context, content cas.Store, id cas.ID) (size int64, result error) {
	reader, err := content.Open(ctx, id)
	if err != nil {
		return 0, err
	}
	defer func() { result = errors.Join(result, reader.Close()) }()
	return io.Copy(io.Discard, reader)
}

// eachChange walks a whole feed, including internal records.
func eachChange(ctx context.Context, read func(context.Context, provenance.ChangeQuery) (provenance.ChangePage, error), visit func(provenance.Change) error) error {
	position := uint64(0)
	for {
		page, err := read(ctx, provenance.ChangeQuery{After: position, Limit: 1000, IncludeInternal: true})
		if err != nil {
			return err
		}
		for _, change := range page.Changes {
			if err := visit(change); err != nil {
				return err
			}
		}
		if len(page.Changes) < 1000 {
			return nil
		}
		position = page.Last
	}
}

// checkRefs reports references whose targets do not resolve in the layers given.
func (s Services) checkRefs(ctx context.Context, layer, id string, finding func(layer, id, format string, args ...any), refs ...provenance.Ref) {
	for _, ref := range refs {
		if err := ref.Validate(); err != nil {
			finding(layer, id, "reference %+v: %v", ref, err)
			continue
		}
		var err error
		switch ref.Form() {
		case provenance.FormContent:
			_, err = s.Artifacts.StatContent(ctx, cas.ID(ref.Digest))
		case provenance.FormOccurrence:
			_, err = s.Artifacts.GetOccurrence(ctx, artifacts.OccurrenceID(ref.Occurrence))
		case provenance.FormAssertion:
			if s.Assertions != nil {
				err = s.Assertions.ValidateAssertion(ctx, ref.Assertion)
			}
		case provenance.FormActivity:
			if s.Lineage != nil {
				err = s.Lineage.ValidateActivity(ctx, ref.Activity)
			}
		case provenance.FormExternal:
		default:
			if ref.Artifact != "" {
				_, err = s.Artifacts.Category(ctx, artifacts.ID(ref.Artifact))
			}
			if err == nil && ref.Revision != "" && s.Lineage != nil {
				err = s.Lineage.ValidateRevision(ctx, ref.Revision, ref.Artifact)
			}
		}
		if err != nil {
			finding(layer, id, "dangling reference %+v: %v", ref, err)
		}
	}
}

func verifyLineage(ctx context.Context, s Services, finding func(layer, id, format string, args ...any), out *Verification) error {
	return eachChange(ctx, s.Lineage.Changes, func(c provenance.Change) error {
		out.RecordsChecked++
		record, err := s.Lineage.Get(ctx, lineage.ID(c.ID))
		if err != nil {
			finding("lineage", c.ID, "%v", err)
			return nil
		}
		switch {
		case record.Activity != nil:
			s.checkRefs(ctx, "lineage", c.ID, finding, record.Activity.Inputs...)
			s.checkRefs(ctx, "lineage", c.ID, finding, record.Activity.Outputs...)
		case record.Relationship != nil:
			s.checkRefs(ctx, "lineage", c.ID, finding, record.Relationship.From, record.Relationship.To)
		case record.Revision != nil:
			s.checkRefs(ctx, "lineage", c.ID, finding, provenance.Ref{Artifact: string(record.Revision.Artifact)})
		}
		return nil
	})
}

func verifyAssertions(ctx context.Context, s Services, finding func(layer, id, format string, args ...any), out *Verification) error {
	return eachChange(ctx, s.Assertions.Changes, func(c provenance.Change) error {
		out.RecordsChecked++
		if c.Class != assertions.ChangeAssertion {
			return nil
		}
		a, err := s.Assertions.Get(ctx, assertions.ID(c.ID))
		if err != nil {
			finding("assertions", c.ID, "%v", err)
			return nil
		}
		s.checkRefs(ctx, "assertions", c.ID, finding, append([]provenance.Ref{a.Subject}, a.Evidence...)...)
		return nil
	})
}
