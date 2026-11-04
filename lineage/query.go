package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/internal/pager"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

func validDependencyRole(role string) bool {
	return role == "" || role == provenance.Value || role == provenance.Predicate || role == provenance.Control
}
func queryValid(q Query) error {
	if q.Kind != "" && q.Kind != "activity" && q.Kind != "relationship" && q.Kind != "revision" {
		return ErrInvalid
	}
	if q.ActivityKind != "" && q.Kind != "" && q.Kind != "activity" {
		return ErrInvalid
	}
	if q.RelationshipKind != "" && q.Kind != "" && q.Kind != "relationship" {
		return ErrInvalid
	}
	roles := map[string]string{"input": "activity", "output": "activity", "from": "relationship", "to": "relationship", "snapshot": "revision", "parent": "revision"}
	if q.ActivityKind != "" && q.RelationshipKind != "" {
		return ErrInvalid
	}
	if q.Role != "" {
		required, ok := roles[q.Role]
		if !ok || q.Ref == nil || (q.Kind != "" && q.Kind != required) || (q.ActivityKind != "" && required != "activity") || (q.RelationshipKind != "" && required != "relationship") {
			return ErrInvalid
		}
	}
	if !validDependencyRole(q.DependencyRole) || q.DependencyRole != "" && (q.Ref == nil || q.Role == "snapshot" || q.Role == "parent" || q.Role == "output" || q.Role == "from" || q.Role == "to") {
		return ErrInvalid
	}
	if q.Activity != "" && (q.Kind == "activity" || q.ActivityKind != "") {
		return ErrInvalid
	}
	if q.Limit < 0 || q.Limit > 1000 {
		return ErrInvalid
	}
	if q.Ref != nil {
		if err := q.Ref.Validate(); err != nil {
			return errors.Join(ErrInvalid, err)
		}
	}
	return nil
}
func matchesRef(want, have provenance.Ref) bool {
	return (want.Artifact == "" || want.Artifact == have.Artifact) && (want.Revision == "" || want.Revision == have.Revision) && (want.Schema == "" || want.Schema == have.Schema) && (want.Row == "" || want.Row == have.Row) && (want.Column == "" || want.Column == have.Column) &&
		(want.Occurrence == "" || want.Occurrence == have.Occurrence) && (want.Digest == "" || want.Digest == have.Digest) && (want.Path == "" || want.Path == have.Path) && (want.Length == 0 || want.Offset == have.Offset && want.Length == have.Length) &&
		(want.Assertion == "" || want.Assertion == have.Assertion) && (want.Activity == "" || want.Activity == have.Activity) && (want.URL == "" || want.URL == have.URL) && (want.Retrieval == "" || want.Retrieval == have.Retrieval)
}
func canonicalRef(ref provenance.Ref) provenance.Ref {
	if ref.Row != "" && ref.Column == "" {
		ref.Schema = ""
	}
	return ref
}
func (s *Service) resolve(ref provenance.Ref) provenance.Ref {
	if ref.Revision != "" && ref.Artifact == "" {
		if p, ok := s.lookup(ID(ref.Revision)); ok && p.Record.Revision != nil {
			ref.Artifact = string(p.Record.Revision.Artifact)
		}
	}
	return canonicalRef(ref)
}
func (s *Service) List(ctx context.Context, q Query) (page Page, result error) {
	ctx, span := start(ctx, "list", "")
	defer func() { span.End(result) }()
	if err := s.requireOpen(); err != nil {
		return page, err
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := queryValid(q); err != nil {
		return page, err
	}
	metadata, err := s.normalizePredicates(q.Metadata)
	if err != nil {
		return page, err
	}
	q.Metadata = metadata
	if q.Cursor == "" && q.Ref != nil && q.Ref.Detailed() {
		if s.options.ValidateSelector == nil {
			return page, ErrUnsupportedSelector
		}
		if err := s.options.ValidateSelector(observability.WithInternal(ctx), *q.Ref); err != nil {
			return page, err
		}
	}
	if q.Limit == 0 {
		q.Limit = 100
	}
	cursor := q.Cursor
	q.Cursor = ""
	sequences, next, err := s.pages.Page(q, cursor, q.Limit, func() ([]uint64, error) {
		scanned := int64(0)
		defer func() {
			if scanned > 0 {
				span.Count("scanned", scanned)
			}
		}()
		type hit struct {
			id       ID
			sequence uint64
		}
		hits := []hit{}
		budget := &mappingBudget{}
		work := 0
		visit := func(p persisted) error {
			scanned++
			match, err := s.matchRecord(ctx, q, p, budget, &work)
			if err != nil {
				return err
			}
			if match {
				hits = append(hits, hit{p.Record.ID, p.Record.Sequence})
			}
			return nil
		}
		if err := s.eachCandidate(ctx, q, visit); err != nil {
			return nil, err
		}
		sort.Slice(hits, func(i, j int) bool { return hits[i].id < hits[j].id })
		ordered := make([]uint64, len(hits))
		for i, h := range hits {
			ordered[i] = h.sequence
		}
		return ordered, nil
	})
	if err != nil {
		return page, queryFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	records := make([]Record, 0, len(sequences))
	for _, sequence := range sequences {
		entry, err := s.journal.At(sequence)
		if err != nil {
			return page, err
		}
		p, err := decodePersisted(entry)
		if err != nil {
			return page, err
		}
		records = append(records, p.Record)
	}
	span.Count("returned", int64(len(records)))
	return Page{Records: records, NextCursor: next}, nil
}

// eachCandidate answers from an index when the query has an indexed predicate, so
// the scanned count is the matching records rather than the whole journal.
func (s *Service) eachCandidate(ctx context.Context, q Query, visit func(persisted) error) error {
	s.mu.RLock()
	var lists [][]uint64
	if q.History != "" {
		lists = append(lists, s.histories.Snapshot(q.History))
	}
	for key, value := range q.Metadata {
		lists = append(lists, s.metadata.Snapshot(journal.IndexKey(key, string(value))))
	}
	s.mu.RUnlock()
	if len(lists) > 0 {
		sort.Slice(lists, func(i, j int) bool { return len(lists[i]) < len(lists[j]) })
		for _, sequence := range lists[0] {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry, err := s.journal.At(sequence)
			if err != nil {
				return err
			}
			p, err := decodePersisted(entry)
			if err != nil {
				return err
			}
			if err := visit(p); err != nil {
				return err
			}
		}
		return nil
	}
	return s.journal.Scan(ctx, 0, 0, func(entry journal.Entry) error {
		p, err := decodePersisted(entry)
		if err != nil {
			return err
		}
		return visit(p)
	})
}

func (s *Service) matchRecord(ctx context.Context, q Query, p persisted, budget *mappingBudget, work *int) (bool, error) {
	if !q.IncludeInternal && p.Category == observability.Internal {
		return false, nil
	}
	r := p.Record
	if q.Kind != "" && kind(r) != q.Kind {
		return false, nil
	}
	if q.ActivityKind != "" && (r.Activity == nil || r.Activity.Kind != q.ActivityKind) {
		return false, nil
	}
	if q.RelationshipKind != "" && (r.Relationship == nil || r.Relationship.Kind != q.RelationshipKind) {
		return false, nil
	}
	if q.History != "" && (r.Revision == nil || r.Revision.History != q.History) {
		return false, nil
	}
	if len(q.Metadata) > 0 {
		metadata := recordMetadata(r)
		for key, value := range q.Metadata {
			have, ok := metadata[key]
			if !ok {
				return false, nil
			}
			normalized, err := canon.IndexedScalar(have)
			if err != nil || !bytes.Equal(normalized, value) {
				return false, nil
			}
		}
	}
	if q.Activity != "" {
		var establishing ID
		if r.Relationship != nil {
			establishing = r.Relationship.Activity
		}
		if r.Revision != nil {
			establishing = r.Revision.Activity
		}
		if establishing != q.Activity {
			return false, nil
		}
	}
	if q.Ref == nil {
		return true, nil
	}
	match := false
	want := s.resolve(*q.Ref)
	check := func(role string, ref provenance.Ref) {
		if (q.Role == "" || q.Role == role) && matchesRef(want, s.resolve(ref)) {
			match = true
		}
	}
	if q.DependencyRole == "" {
		if a := r.Activity; a != nil {
			for _, ref := range a.Inputs {
				check("input", ref)
			}
			for _, ref := range a.Outputs {
				check("output", ref)
			}
		}
		if e := r.Relationship; e != nil {
			check("from", e.From)
			check("to", e.To)
		}
		if v := r.Revision; v != nil {
			check("snapshot", provenance.Ref{Artifact: string(v.Artifact), Revision: string(r.ID)})
			for _, parent := range v.Parents {
				check("parent", provenance.Ref{Revision: string(parent)})
			}
		}
	}
	if !match && (q.Role == "" || q.Role == "input" || q.Role == "output") {
		err := s.eachMappingBudget(ctx, r, budget, func(_ int, _ Evidence, m provenance.Mapping) error {
			*work += len(m.Inputs) + 1
			if *work > 100000 {
				return ErrLimit
			}
			if q.DependencyRole == "" && (q.Role == "" || q.Role == "output") && matchesRef(want, s.resolve(m.Output)) {
				match = true
			}
			for _, d := range m.Inputs {
				if (q.DependencyRole == "" || q.DependencyRole == d.Role) && (q.Role == "" || q.Role == "input") && matchesRef(want, s.resolve(d.Ref)) {
					match = true
				}
			}
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	return match, nil
}

type traversalItem struct {
	Node      *Node
	Edge      *Edge
	Truncated bool
	Reason    string
	Coverage  []provenance.Coverage
}

func normalizeTraversal(q *TraversalQuery) error {
	if err := q.Root.Validate(); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	if q.Direction == "" {
		q.Direction = "outgoing"
	}
	if q.Direction != "outgoing" && q.Direction != "incoming" && q.Direction != "both" {
		return ErrInvalid
	}
	if len(q.Kinds) == 0 {
		q.Kinds = []string{"derived-from"}
	}
	if len(q.Kinds) > 32 {
		return ErrLimit
	}
	q.Kinds = append([]string(nil), q.Kinds...)
	sort.Strings(q.Kinds)
	for i, k := range q.Kinds {
		if k == "" || (i > 0 && k == q.Kinds[i-1]) {
			return ErrInvalid
		}
	}
	if q.MaxDepth == 0 {
		q.MaxDepth = 4
	}
	if q.MaxNodes == 0 {
		q.MaxNodes = 1000
	}
	if q.MaxResults == 0 {
		q.MaxResults = 2000
	}
	if q.Limit == 0 {
		q.Limit = 100
	}
	if q.MaxDepth < 0 || q.MaxDepth > 32 || q.MaxNodes < 1 || q.MaxNodes > 10000 || q.MaxResults < 1 || q.MaxResults > 20000 || q.Limit < 1 || q.Limit > 1000 || !validDependencyRole(q.DependencyRole) {
		return ErrInvalid
	}
	return nil
}
func (s *Service) Traverse(ctx context.Context, q TraversalQuery) (page TraversalPage, result error) {
	ctx, span := start(ctx, "traverse", "")
	defer func() { span.End(result) }()
	if err := s.requireOpen(); err != nil {
		return page, err
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := normalizeTraversal(&q); err != nil {
		return page, err
	}
	cursor := q.Cursor
	q.Cursor = ""
	if cursor == "" {
		if err := s.validateRef(observability.WithInternal(ctx), q.Root); err != nil {
			return page, err
		}
	}
	items, next, err := s.traversals.Page(q, cursor, q.Limit, func() ([]traversalItem, error) { return s.traverseSnapshot(ctx, q) })
	if err != nil {
		return page, queryFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	page.NextCursor = next
	for _, item := range items {
		if item.Node != nil {
			page.Nodes = append(page.Nodes, *item.Node)
		}
		if item.Edge != nil {
			page.Edges = append(page.Edges, *item.Edge)
		}
		page.Truncated = item.Truncated
		page.Reason = item.Reason
		if len(item.Coverage) > 0 {
			page.Coverage = item.Coverage
		}
	}
	span.Count("returned_nodes", int64(len(page.Nodes)))
	span.Count("returned_edges", int64(len(page.Edges)))
	return page, nil
}

// recordMetadata returns a record's caller metadata, whichever payload it has.
func recordMetadata(r Record) map[string]json.RawMessage {
	switch {
	case r.Activity != nil:
		return r.Activity.Metadata
	case r.Relationship != nil:
		return r.Relationship.Metadata
	case r.Revision != nil:
		return r.Revision.Metadata
	}
	return nil
}

func (s *Service) traverseSnapshot(ctx context.Context, q TraversalQuery) ([]traversalItem, error) {
	// Records stream from the journal, bounded to the sequence at the start. Only
	// edges within the work budget, and revisions actually mentioned, stay resident.
	watermark := s.journal.Sequence()
	records := map[ID]*persisted{}
	lookup := func(id ID) *persisted {
		if p, ok := records[id]; ok {
			return p
		}
		var found *persisted
		if p, ok := s.lookup(id); ok && p.Record.Sequence <= watermark {
			found = &p
		}
		records[id] = found
		return found
	}
	resolve := func(ref provenance.Ref) provenance.Ref {
		if ref.Revision != "" && ref.Artifact == "" {
			if p := lookup(ID(ref.Revision)); p != nil && p.Record.Revision != nil {
				ref.Artifact = string(p.Record.Revision.Artifact)
			}
		}
		return canonicalRef(ref)
	}
	// All adjacency is bounded independently of user result/page limits.
	type candidate struct {
		edge     Edge
		internal bool
	}
	adjacency := map[provenance.Ref][]candidate{}
	budget := &mappingBudget{}
	total := 0
	workBytes := 0
	coverages := []provenance.Coverage{}
	coverageNodes := map[provenance.Ref]map[string]provenance.Coverage{}
	selectedCoverage := map[string]provenance.Coverage{}
	selected := map[string]bool{}
	for _, k := range q.Kinds {
		selected[k] = true
	}
	add := func(e Edge, internal bool) error {
		if !selected[e.Kind] || q.DependencyRole != "" && e.DependencyRole != q.DependencyRole {
			return nil
		}
		total++
		workBytes += len(e.Expression) + len(e.ID) + e.From.Weight() + e.To.Weight() + 128
		if total > 100000 || workBytes > 32<<20 {
			return ErrLimit
		}
		e.From = resolve(e.From)
		e.To = resolve(e.To)
		c := candidate{edge: e, internal: internal}
		if q.Direction == "outgoing" || q.Direction == "both" {
			adjacency[e.From] = append(adjacency[e.From], c)
		}
		if q.Direction == "incoming" || q.Direction == "both" {
			adjacency[e.To] = append(adjacency[e.To], c)
		}
		return nil
	}
	err := s.journal.Scan(ctx, 0, watermark, func(entry journal.Entry) error {
		p, err := decodePersisted(entry)
		if err != nil {
			return err
		}
		r := p.Record
		internal := p.Category == observability.Internal
		if e := r.Relationship; e != nil {
			if err := add(Edge{ID: string(r.ID), Kind: e.Kind, From: e.From, To: e.To, Activity: e.Activity}, internal); err != nil {
				return err
			}
		}
		if v := r.Revision; v != nil {
			for _, parent := range v.Parents {
				if err := add(Edge{ID: fmt.Sprintf("%s/%s", r.ID, parent), Kind: "revision-of", From: provenance.Ref{Revision: string(r.ID), Artifact: string(v.Artifact)}, To: provenance.Ref{Revision: string(parent)}, Activity: v.Activity}, internal); err != nil {
					return err
				}
			}
		}
		if selected["derived-from"] && (q.Root.Detailed() || q.DependencyRole != "") {

			err := s.eachMappingBudget(ctx, r, budget, func(index int, e Evidence, m provenance.Mapping) error {
				coverageID := string(r.ID) + "/" + string(e.Artifact) + "/" + e.Path
				refs := []provenance.Ref{m.Output}
				for _, d := range m.Inputs {
					if q.DependencyRole == "" || q.DependencyRole == d.Role {
						refs = append(refs, d.Ref)
					}
				}
				for _, ref := range refs {
					ref = resolve(ref)
					if coverageNodes[ref] == nil {
						coverageNodes[ref] = map[string]provenance.Coverage{}
					}
					if _, ok := coverageNodes[ref][coverageID]; !ok {
						workBytes += 256 + len(coverageID)
						if workBytes > 32<<20 {
							return ErrLimit
						}
						coverageNodes[ref][coverageID] = e.Coverage
					}
				}
				for i, d := range m.Inputs {
					copy := e
					activity := r.ID
					if r.Relationship != nil {
						activity = r.Relationship.Activity
					}
					if err := add(Edge{ID: fmt.Sprintf("%s/mapping/%d/%d", r.ID, index, i), Kind: "derived-from", From: m.Output, To: d.Ref, Activity: activity, DependencyRole: d.Role, Evidence: &copy, Expression: m.Expression}, internal); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for key := range adjacency {
		sort.Slice(adjacency[key], func(i, j int) bool {
			a, b := adjacency[key][i].edge, adjacency[key][j].edge
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			return a.ID < b.ID
		})
	}
	root := resolve(q.Root)
	queue := []Node{{Ref: root, Depth: 0}}
	seenNodes := map[provenance.Ref]bool{root: true}
	// Edge IDs join record IDs with '/', and record IDs may themselves contain
	// '/', so identity must include the resolved endpoints, not just the ID text.
	type edgeKey struct {
		kind, id string
		from, to provenance.Ref
	}
	keyOf := func(e Edge) edgeKey { return edgeKey{e.Kind, e.ID, e.From, e.To} }
	seenEdges := map[edgeKey]bool{}
	nodes := []Node{}
	edges := []Edge{}
	truncated := false
	reason := ""
	visibility := map[string]bool{}
	var visibilityErr error
	visible := func(ref provenance.Ref) bool {
		if q.IncludeInternal {
			return true
		}
		if ref.Revision != "" {
			if p := lookup(ID(ref.Revision)); p != nil && p.Category == observability.Internal {
				return false
			}
		}
		if ref.Occurrence != "" {
			key := "occurrence/" + ref.Occurrence
			if v, ok := visibility[key]; ok {
				return v
			}
			occurrence, e := s.artifacts.GetOccurrence(observability.WithInternal(ctx), artifacts.OccurrenceID(ref.Occurrence))
			if e != nil {
				visibilityErr = e
				return false
			}
			visibility[key] = occurrence.Category != observability.Internal
			return visibility[key]
		}
		if ref.Artifact != "" {
			if v, ok := visibility[ref.Artifact]; ok {
				return v
			}
			category, e := s.artifacts.Category(observability.WithInternal(ctx), artifacts.ID(ref.Artifact))
			if e != nil {
				visibilityErr = e
				return false
			}
			v := category != observability.Internal
			visibility[ref.Artifact] = v
			return v
		}
		return true
	}
	if visible(root) {
		nodes = append(nodes, queue[0])
	}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		node := queue[0]
		queue = queue[1:]
		if visible(node.Ref) {
			for id, coverage := range coverageNodes[node.Ref] {
				selectedCoverage[id] = coverage
				if len(selectedCoverage) > 64 {
					return nil, ErrLimit
				}
			}
		}
		if node.Depth >= q.MaxDepth {
			for _, c := range adjacency[node.Ref] {
				if !seenEdges[keyOf(c.edge)] {
					truncated = true
					if reason == "" {
						reason = "depth"
					}
					break
				}
			}
			continue
		}
		for _, c := range adjacency[node.Ref] {
			e := c.edge
			identity := keyOf(e)
			if seenEdges[identity] {
				continue
			}
			if len(seenEdges) >= q.MaxResults {
				truncated = true
				reason = "results"
				queue = nil
				break
			}
			other := e.To
			if other == node.Ref {
				other = e.From
			}
			if !seenNodes[other] && len(seenNodes) >= q.MaxNodes {
				truncated = true
				reason = "nodes"
				continue
			}
			seenEdges[identity] = true
			if !seenNodes[other] {
				seenNodes[other] = true
				next := Node{Ref: other, Depth: node.Depth + 1}
				queue = append(queue, next)
				if visible(other) {
					nodes = append(nodes, next)
				}
			}
			if (q.IncludeInternal || !c.internal) && visible(e.From) && visible(e.To) {
				edges = append(edges, e)
			}
		}
	}
	if visibilityErr != nil {
		return nil, visibilityErr
	}
	coverageIDs := make([]string, 0, len(selectedCoverage))
	for id := range selectedCoverage {
		coverageIDs = append(coverageIDs, id)
	}
	sort.Strings(coverageIDs)
	for _, id := range coverageIDs {
		coverages = append(coverages, selectedCoverage[id])
	}
	// One bounded item may carry one node and one edge; both page collections are
	// independently <= Limit. Repeat snapshot metadata so every page is truthful.
	count := len(nodes)
	if len(edges) > count {
		count = len(edges)
	}
	if count == 0 {
		count = 1
	}
	items := make([]traversalItem, count)
	for i := range items {
		item := &items[i]
		item.Truncated = truncated
		item.Reason = reason
		item.Coverage = coverages
		if i < len(nodes) {
			n := nodes[i]
			item.Node = &n
		}
		if i < len(edges) {
			e := edges[i]
			item.Edge = &e
		}
	}
	return items, nil
}

func queryFailure(err error) error {
	if errors.Is(err, pager.ErrCursor) {
		return errors.Join(ErrCursor, ErrInvalid, err)
	}
	if errors.Is(err, pager.ErrCapacity) {
		return errors.Join(ErrLimit, err)
	}
	return err
}
