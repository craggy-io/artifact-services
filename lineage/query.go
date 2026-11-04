package lineage

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/craggy-io/artifact-services/artifacts"
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
	if !validDependencyRole(q.DependencyRole) || q.DependencyRole != "" && (q.Ref == nil || q.Role == "snapshot" || q.Role == "parent") {
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
	return (want.Artifact == "" || want.Artifact == have.Artifact) && (want.Revision == "" || want.Revision == have.Revision) && (want.Schema == "" || want.Schema == have.Schema) && (want.Row == "" || want.Row == have.Row) && (want.Column == "" || want.Column == have.Column)
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
	if q.Limit == 0 {
		q.Limit = 100
	}
	cursor := q.Cursor
	q.Cursor = ""
	records, next, err := s.pages.Page(q, cursor, q.Limit, func() ([]Record, error) {
		values, err := s.snapshot()
		if err != nil {
			return nil, err
		}
		out := []Record{}
		work := 0
		for _, p := range values {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !q.IncludeInternal && p.Category == observability.Internal {
				continue
			}
			r := p.Record
			if q.Kind != "" && kind(r) != q.Kind {
				continue
			}
			if q.ActivityKind != "" && (r.Activity == nil || r.Activity.Kind != q.ActivityKind) {
				continue
			}
			if q.RelationshipKind != "" && (r.Relationship == nil || r.Relationship.Kind != q.RelationshipKind) {
				continue
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
					continue
				}
			}
			match := q.Ref == nil
			if q.Ref != nil {
				want := s.resolve(*q.Ref)
				visit := func(role string, ref provenance.Ref) {
					if (q.Role == "" || q.Role == role) && matchesRef(want, s.resolve(ref)) {
						match = true
					}
				}
				if q.DependencyRole == "" {
					if a := r.Activity; a != nil {
						for _, ref := range a.Inputs {
							visit("input", ref)
						}
						for _, ref := range a.Outputs {
							visit("output", ref)
						}
					}
					if e := r.Relationship; e != nil {
						visit("from", e.From)
						visit("to", e.To)
					}
					if v := r.Revision; v != nil {
						visit("snapshot", provenance.Ref{Artifact: string(v.Artifact)})
						for _, parent := range v.Parents {
							visit("parent", provenance.Ref{Revision: string(parent)})
						}
					}
				}
				if !match && (q.Ref.Detailed() || q.DependencyRole != "") {
					err = s.eachMapping(ctx, r, func(_ int, _ Evidence, m provenance.Mapping) error {
						work += len(m.Inputs) + 1
						if work > 100000 {
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
						return nil, err
					}
				}
			}
			span.Count("scanned", 1)
			if match {
				out = append(out, r)
			}
		}
		return out, nil
	})
	if err != nil {
		return page, queryFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	span.Count("returned", int64(len(records)))
	return Page{Records: records, NextCursor: next}, nil
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
func (s *Service) traverseSnapshot(ctx context.Context, q TraversalQuery) ([]traversalItem, error) {
	values, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	records := map[ID]persisted{}
	for _, p := range values {
		records[p.Record.ID] = p
	}
	resolve := func(ref provenance.Ref) provenance.Ref {
		if ref.Revision != "" && ref.Artifact == "" {
			if p, ok := records[ID(ref.Revision)]; ok && p.Record.Revision != nil {
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
		workBytes += len(e.Expression) + len(e.ID) + len(e.From.Artifact) + len(e.From.Revision) + len(e.From.Schema) + len(e.From.Row) + len(e.From.Column) + len(e.To.Artifact) + len(e.To.Revision) + len(e.To.Schema) + len(e.To.Row) + len(e.To.Column) + 128
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
	for _, p := range values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r := p.Record
		internal := p.Category == observability.Internal
		if e := r.Relationship; e != nil {
			if err := add(Edge{ID: string(r.ID), Kind: e.Kind, From: e.From, To: e.To, Activity: e.Activity}, internal); err != nil {
				return nil, err
			}
		}
		if v := r.Revision; v != nil {
			for _, parent := range v.Parents {
				if err := add(Edge{ID: fmt.Sprintf("%s/%s", r.ID, parent), Kind: "revision-of", From: provenance.Ref{Revision: string(r.ID), Artifact: string(v.Artifact)}, To: provenance.Ref{Revision: string(parent)}, Activity: v.Activity}, internal); err != nil {
					return nil, err
				}
			}
		}
		if selected["derived-from"] && (q.Root.Detailed() || q.DependencyRole != "") {

			err := s.eachMapping(ctx, r, func(index int, e Evidence, m provenance.Mapping) error {
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
					if err := add(Edge{ID: fmt.Sprintf("%s/mapping/%d/%d", r.ID, index, i), Kind: "derived-from", From: m.Output, To: d.Ref, Activity: activity, DependencyRole: d.Role, Evidence: &copy, Expression: m.Expression}, false); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
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
	seenEdges := map[string]bool{}
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
			if p, ok := records[ID(ref.Revision)]; ok && p.Category == observability.Internal {
				return false
			}
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
			if len(adjacency[node.Ref]) > 0 {
				truncated = true
				if reason == "" {
					reason = "depth"
				}
			}
			continue
		}
		for _, c := range adjacency[node.Ref] {
			e := c.edge
			identity := e.Kind + ":" + e.ID
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
		return errors.Join(ErrInvalid, err)
	}
	if errors.Is(err, pager.ErrCapacity) {
		return errors.Join(ErrLimit, err)
	}
	return err
}
