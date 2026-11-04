package objects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
	"io"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

type queryTable struct {
	input      Input
	schema     artifacts.ID
	definition SchemaDefinition
	rows       []Row
}
type boundRow struct {
	table *queryTable
	row   Row
}
type tuple struct {
	rows       map[string]boundRow
	predicates []provenance.Dependency
}
type evaluated struct {
	value any
	deps  []provenance.Dependency
}

func dependencyKey(d provenance.Dependency) string { b, _ := json.Marshal(d); return string(b) }
func union(groups ...[]provenance.Dependency) []provenance.Dependency {
	m := map[string]provenance.Dependency{}
	for _, group := range groups {
		for _, d := range group {
			m[dependencyKey(d)] = d
		}
	}
	keys := sortedKeys(m)
	result := make([]provenance.Dependency, 0, len(keys))
	for _, k := range keys {
		result = append(result, m[k])
	}
	return result
}
func role(deps []provenance.Dependency, kind string) []provenance.Dependency {
	result := make([]provenance.Dependency, len(deps))
	for i, d := range deps {
		d.Role = kind
		result[i] = d
	}
	return result
}
func rowRef(b boundRow) provenance.Ref {
	return provenance.Ref{Artifact: string(b.table.input.Artifact), Revision: b.table.input.Revision, Row: b.row.ID}
}
func rowDeps(t tuple) []provenance.Dependency {
	var result []provenance.Dependency
	for _, alias := range sortedKeys(t.rows) {
		result = append(result, provenance.Dependency{Ref: rowRef(t.rows[alias]), Role: provenance.Value})
	}
	return result
}
func rat(value any) (*big.Rat, bool) {
	var text string
	switch n := value.(type) {
	case json.Number:
		text = string(n)
	case int:
		text = strconv.Itoa(n)
	case int64:
		text = strconv.FormatInt(n, 10)
	case float64:
		text = strconv.FormatFloat(n, 'g', -1, 64)
	default:
		return nil, false
	}
	r, ok := new(big.Rat).SetString(text)
	return r, ok
}
func ratValue(r *big.Rat) (any, error) {
	if r.IsInt() {
		return json.Number(r.Num().String()), nil
	}
	den := new(big.Int).Set(r.Denom())
	counts := [2]int{}
	for i, f := range []*big.Int{big.NewInt(2), big.NewInt(5)} {
		for new(big.Int).Mod(den, f).Sign() == 0 {
			den.Div(den, f)
			counts[i]++
			if counts[i] > 32 {
				return nil, fmt.Errorf("%w: decimal precision limit", ErrLimit)
			}
		}
	}
	if den.Cmp(big.NewInt(1)) != 0 {
		return nil, fmt.Errorf("%w: nonterminating decimal division", ErrUnsupported)
	}
	digits := counts[0]
	if counts[1] > digits {
		digits = counts[1]
	}
	text := strings.TrimRight(strings.TrimRight(r.FloatString(digits), "0"), ".")
	return json.Number(text), nil
}
func compareValues(a, b any) (int, bool, error) {
	if a == nil || b == nil {
		return 0, false, nil
	}
	if x, ok := rat(a); ok {
		y, ok := rat(b)
		if !ok {
			return 0, false, ErrQuery
		}
		return x.Cmp(y), true, nil
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		if !ok {
			return 0, false, ErrQuery
		}
		return strings.Compare(x, y), true, nil
	case bool:
		y, ok := b.(bool)
		if !ok {
			return 0, false, ErrQuery
		}
		if x == y {
			return 0, true, nil
		}
		if !x {
			return -1, true, nil
		}
		return 1, true, nil
	}
	return 0, false, fmt.Errorf("%w: scalar comparison required", ErrUnsupported)
}
func resolveColumn(t tuple, name string) (evaluated, error) {
	parts := strings.Split(name, ".")
	var found *evaluated
	for alias, b := range t.rows {
		if len(parts) == 2 && parts[0] != alias {
			continue
		}
		columnName := parts[len(parts)-1]
		for _, c := range b.table.definition.Columns {
			if strings.EqualFold(c.Name, columnName) {
				if found != nil {
					return evaluated{}, fmt.Errorf("%w: ambiguous column %s", ErrQuery, name)
				}
				ref := rowRef(b)
				ref.Schema = string(b.table.schema)
				ref.Column = c.ID
				v := evaluated{b.row.Values[c.ID], []provenance.Dependency{{Ref: ref, Role: provenance.Value}}}
				found = &v
			}
		}
	}
	if found == nil {
		return evaluated{}, fmt.Errorf("%w: unknown column %s", ErrQuery, name)
	}
	return *found, nil
}
func evaluate(e *expression, t tuple, group []tuple, params map[string]any) (evaluated, error) {
	if e == nil {
		return evaluated{value: true}, nil
	}
	switch e.kind {
	case "literal":
		return evaluated{value: e.value}, nil
	case "param":
		v, ok := params[e.text]
		if !ok {
			return evaluated{}, fmt.Errorf("%w: missing parameter %s", ErrQuery, e.text)
		}
		return evaluated{value: v}, nil
	case "column":
		return resolveColumn(t, e.text)
	case "aggregate":
		var deps []provenance.Dependency
		sum := new(big.Rat)
		count := int64(0)
		for _, member := range group {
			if e.left.kind == "star" {
				if e.text != "count" {
					return evaluated{}, ErrUnsupported
				}
				count++
				deps = union(deps, rowDeps(member))
				continue
			}
			v, err := evaluate(e.left, member, nil, params)
			if err != nil {
				return evaluated{}, err
			}
			deps = union(deps, v.deps)
			if v.value == nil {
				continue
			}
			count++
			if e.text == "sum" {
				n, ok := rat(v.value)
				if !ok {
					return evaluated{}, fmt.Errorf("%w: SUM requires numbers", ErrQuery)
				}
				sum.Add(sum, n)
			}
		}
		if e.text == "count" {
			return evaluated{json.Number(strconv.FormatInt(count, 10)), deps}, nil
		}
		if count == 0 {
			return evaluated{nil, deps}, nil
		}
		value, err := ratValue(sum)
		return evaluated{value, deps}, err
	case "unary":
		v, err := evaluate(e.left, t, group, params)
		if err != nil || v.value == nil {
			return v, err
		}
		n, ok := rat(v.value)
		if !ok {
			return evaluated{}, ErrQuery
		}
		if e.text == "-" {
			n.Neg(n)
		}
		v.value, err = ratValue(n)
		return v, err
	case "binary":
		left, err := evaluate(e.left, t, group, params)
		if err != nil {
			return evaluated{}, err
		}
		right, err := evaluate(e.right, t, group, params)
		if err != nil {
			return evaluated{}, err
		}
		deps := union(left.deps, right.deps)
		switch e.text {
		case "AND":
			a, ok := left.value.(bool)
			b, ok2 := right.value.(bool)
			if !ok || !ok2 {
				return evaluated{}, ErrQuery
			}
			return evaluated{a && b, deps}, nil
		case "+", "-", "*", "/":
			if left.value == nil || right.value == nil {
				return evaluated{nil, deps}, nil
			}
			a, ok := rat(left.value)
			b, ok2 := rat(right.value)
			if !ok || !ok2 {
				return evaluated{}, fmt.Errorf("%w: arithmetic requires numbers", ErrQuery)
			}
			switch e.text {
			case "+":
				a.Add(a, b)
			case "-":
				a.Sub(a, b)
			case "*":
				a.Mul(a, b)
			case "/":
				if b.Sign() == 0 {
					return evaluated{}, fmt.Errorf("%w: division by zero", ErrQuery)
				}
				a.Quo(a, b)
			}
			value, err := ratValue(a)
			return evaluated{value, deps}, err
		default:
			cmp, known, err := compareValues(left.value, right.value)
			if err != nil {
				return evaluated{}, err
			}
			match := false
			if known {
				switch e.text {
				case "=":
					match = cmp == 0
				case "!=", "<>":
					match = cmp != 0
				case "<":
					match = cmp < 0
				case ">":
					match = cmp > 0
				case "<=":
					match = cmp <= 0
				case ">=":
					match = cmp >= 0
				default:
					return evaluated{}, ErrUnsupported
				}
			}
			return evaluated{match, deps}, nil
		}
	}
	return evaluated{}, ErrUnsupported
}
func normalizeRequest(q QueryRequest) (QueryRequest, error) {
	data, err := canon.Marshal(q)
	if err != nil {
		return q, err
	}
	if len(data) > 1<<20 {
		return q, ErrLimit
	}
	if err = strictJSON(data, &q); err != nil {
		return q, err
	}
	if len(q.Inputs) == 0 || len(q.Inputs) > 8 {
		return q, ErrQuery
	}
	aliases := map[string]bool{}
	for i, in := range q.Inputs {
		alias := strings.ToLower(in.Alias)
		if !identifier.MatchString(alias) || aliases[alias] || in.Revision == "" || len(in.Revision) > 1024 {
			return q, fmt.Errorf("%w: unique aliases and exact input revisions required", ErrQuery)
		}
		if _, err = artifacts.ParseID(string(in.Artifact)); err != nil {
			return q, err
		}
		aliases[alias] = true
		q.Inputs[i].Alias = alias
	}
	return q, nil
}
func (s *Service) loadTable(ctx context.Context, in Input) (*queryTable, error) {
	d, schema, m, encoding, err := s.typed(ctx, in.Artifact)
	if err != nil {
		return nil, err
	}
	if d.Model != Table || encoding != NDJSON {
		return nil, ErrUnsupported
	}
	table := &queryTable{input: in, schema: schema, definition: d}
	err = s.rows(ctx, in.Artifact, m.Entries[0].Path, d, func(row Row) error { table.rows = append(table.rows, row); return nil })
	return table, err
}
func (s *Service) executeSQL(ctx context.Context, q QueryRequest) ([]ResultRow, error) {
	plan, err := parseSQL(q.Text)
	if err != nil {
		return nil, err
	}
	tables := map[string]*queryTable{}
	for _, in := range q.Inputs {
		t, err := s.loadTable(ctx, in)
		if err != nil {
			return nil, err
		}
		tables[in.Alias] = t
	}
	base, ok := tables[plan.from.input]
	if !ok {
		return nil, fmt.Errorf("%w: FROM input", ErrQuery)
	}
	// Validate columns and parameters even for empty inputs; use placeholder rows.
	template := tuple{rows: map[string]boundRow{}}
	rels := append([]relation{plan.from}, plan.joins...)
	used := map[string]bool{}
	for _, r := range rels {
		table, ok := tables[r.input]
		if !ok {
			return nil, ErrQuery
		}
		used[r.input] = true
		values := map[string]any{}
		for _, c := range table.definition.Columns {
			values[c.ID] = nil
		}
		template.rows[r.alias] = boundRow{table, Row{ID: "validation", Values: values}}
		if err = validateExpression(r.on, template, q.Parameters); err != nil {
			return nil, err
		}
	}
	if len(used) != len(tables) {
		return nil, fmt.Errorf("%w: unused pinned input", ErrQuery)
	}
	for _, p := range plan.selects {
		if err = validateExpression(p.expression, template, q.Parameters); err != nil {
			return nil, err
		}
	}
	for _, g := range plan.groups {
		if err = validateExpression(g, template, q.Parameters); err != nil {
			return nil, err
		}
	}
	if err = validateExpression(plan.where, template, q.Parameters); err != nil {
		return nil, err
	}
	for _, r := range plan.joins {
		if err = validateExpression(r.on, template, q.Parameters); err != nil {
			return nil, err
		}
	}
	tuples := make([]tuple, 0, len(base.rows))
	for _, row := range base.rows {
		tuples = append(tuples, tuple{rows: map[string]boundRow{plan.from.alias: {base, row}}})
	}
	work := 0
	for _, join := range plan.joins {
		table := tables[join.input]
		var next []tuple
		for _, left := range tuples {
			for _, right := range table.rows {
				if err = ctx.Err(); err != nil {
					return nil, err
				}
				work++
				if work > s.limits.MaxJoinPairs {
					return nil, ErrLimit
				}
				candidate := tuple{rows: map[string]boundRow{}, predicates: left.predicates}
				for alias, row := range left.rows {
					candidate.rows[alias] = row
				}
				candidate.rows[join.alias] = boundRow{table, right}
				test, err := evaluate(join.on, candidate, nil, q.Parameters)
				if err != nil {
					return nil, err
				}
				if test.value == true {
					candidate.predicates = union(candidate.predicates, role(test.deps, provenance.Predicate))
					next = append(next, candidate)
					if len(next) > s.limits.MaxRows {
						return nil, ErrLimit
					}
				}
			}
		}
		tuples = next
	}
	filtered := tuples[:0]
	for _, t := range tuples {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		test, err := evaluate(plan.where, t, nil, q.Parameters)
		if err != nil {
			return nil, err
		}
		if test.value == true {
			t.predicates = union(t.predicates, role(test.deps, provenance.Predicate))
			filtered = append(filtered, t)
		}
	}
	tuples = filtered
	aggregated := len(plan.groups) > 0
	for _, p := range plan.selects {
		aggregated = aggregated || hasAggregate(p.expression)
	}
	groups := map[string][]tuple{}
	var order []string
	if !aggregated {
		for i, t := range tuples {
			key := strconv.Itoa(i)
			order = append(order, key)
			groups[key] = []tuple{t}
		}
	} else if len(plan.groups) == 0 {
		groups["all"] = tuples
		order = []string{"all"}
	} else {
		for _, t := range tuples {
			values := []any{}
			for _, g := range plan.groups {
				v, err := evaluate(g, t, nil, q.Parameters)
				if err != nil {
					return nil, err
				}
				values = append(values, v.value)
				t.predicates = union(t.predicates, role(v.deps, provenance.Control))
			}
			b, err := canon.Marshal(values)
			if err != nil {
				return nil, err
			}
			key := string(b)
			if _, ok := groups[key]; !ok {
				if len(groups) >= s.limits.MaxGroups {
					return nil, ErrLimit
				}
				order = append(order, key)
			}
			groups[key] = append(groups[key], t)
		}
		sort.Strings(order)
	}
	if len(order) > s.limits.MaxResultRows {
		return nil, ErrLimit
	}
	planBytes, _ := canon.Marshal(q)
	totalDeps := 0
	results := make([]ResultRow, 0, len(order))
	for _, key := range order {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		members := groups[key]
		representative := template
		if len(members) > 0 {
			representative = members[0]
		}
		result := ResultRow{Values: map[string]any{}, Columns: map[string][]provenance.Dependency{}, Expressions: map[string]string{}}
		var rows, predicates []provenance.Dependency
		for _, member := range members {
			rows = union(rows, rowDeps(member))
			predicates = union(predicates, member.predicates)
		}
		for _, p := range plan.selects {
			v, err := evaluate(p.expression, representative, members, q.Parameters)
			if err != nil {
				return nil, err
			}
			result.Values[p.alias] = v.value
			result.Columns[p.alias] = union(v.deps, predicates)
			result.Expressions[p.alias] = expressionText(p.expression)
			result.Dependencies = union(result.Dependencies, v.deps)
		}
		result.Dependencies = union(result.Dependencies, rows, predicates)
		identity := sha256.New()
		identity.Write(planBytes)
		if aggregated {
			identity.Write([]byte(key))
		}
		for _, member := range members {
			bindings := map[string]provenance.Ref{}
			for alias, bound := range member.rows {
				bindings[alias] = rowRef(bound)
			}
			encoded, err := canon.Marshal(bindings)
			if err != nil {
				return nil, err
			}
			identity.Write(encoded)
		}
		result.ID = hex.EncodeToString(identity.Sum(nil))
		totalDeps += len(result.Dependencies)
		for _, d := range result.Columns {
			totalDeps += len(d)
		}
		if totalDeps > s.limits.MaxDependencies {
			return nil, ErrLimit
		}
		results = append(results, result)
	}
	return results, nil
}
func validateExpression(e *expression, t tuple, params map[string]any) error {
	if e == nil {
		return nil
	}
	if e.kind == "column" {
		_, err := resolveColumn(t, e.text)
		return err
	}
	if e.kind == "param" {
		if _, ok := params[e.text]; !ok {
			return fmt.Errorf("%w: missing parameter", ErrQuery)
		}
	}
	if e.kind == "aggregate" && e.left.kind == "star" && e.text != "count" {
		return ErrUnsupported
	}
	if err := validateExpression(e.left, t, params); err != nil {
		return err
	}
	return validateExpression(e.right, t, params)
}
func (s *Service) executeJSON(ctx context.Context, q QueryRequest) ([]ResultRow, error) {
	if len(q.Inputs) != 1 || len(q.Parameters) != 0 {
		return nil, ErrUnsupported
	}
	if q.RequireComplete {
		return nil, fmt.Errorf("%w: JSON pointer lacks table row/column lineage", ErrUnsupported)
	}
	in := q.Inputs[0]
	d, schema, m, encoding, err := s.typed(ctx, in.Artifact)
	if err != nil {
		return nil, err
	}
	if d.Model != JSON || encoding != JSONEncoding {
		return nil, ErrUnsupported
	}
	data, err := s.readFile(observability.WithInternal(ctx), in.Artifact, m.Entries[0].Path, s.limits.MaxBytes)
	if err != nil {
		return nil, err
	}
	var value any
	if err = strictJSON(data, &value); err != nil {
		return nil, err
	}
	if err = checkValue(value, *d.JSON, "$"); err != nil {
		return nil, err
	}
	if q.Text != "" {
		if !strings.HasPrefix(q.Text, "/") {
			return nil, ErrQuery
		}
		for _, part := range strings.Split(q.Text[1:], "/") {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			decoded, err := pointerToken(part)
			if err != nil {
				return nil, err
			}
			switch v := value.(type) {
			case map[string]any:
				var ok bool
				value, ok = v[decoded]
				if !ok {
					return nil, fmt.Errorf("%w: missing JSON pointer", ErrQuery)
				}
			case []any:
				if decoded == "" || len(decoded) > 1 && decoded[0] == '0' {
					return nil, ErrQuery
				}
				n, e := strconv.Atoi(decoded)
				if e != nil || n < 0 || n >= len(v) {
					return nil, ErrQuery
				}
				value = v[n]
			default:
				return nil, ErrQuery
			}
		}
	}
	key, _ := canon.Marshal(q)
	sum := sha256.Sum256(key)
	deps := []provenance.Dependency{{Ref: provenance.Ref{Artifact: string(in.Artifact), Revision: in.Revision, Schema: string(schema)}, Role: provenance.Value}}
	return []ResultRow{{ID: hex.EncodeToString(sum[:]), Values: map[string]any{"value": value}, Dependencies: deps, Columns: map[string][]provenance.Dependency{"value": deps}, Expressions: map[string]string{"value": q.Text}}}, nil
}
func pointerToken(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '~' {
			b.WriteByte(s[i])
			continue
		}
		i++
		if i >= len(s) {
			return "", ErrQuery
		}
		switch s[i] {
		case '0':
			b.WriteByte('~')
		case '1':
			b.WriteByte('/')
		default:
			return "", ErrQuery
		}
	}
	return b.String(), nil
}

type resultStream struct {
	ctx         context.Context
	span        *observability.Span
	rows        []ResultRow
	index       int
	closed, eof bool
	err         error
	coverage    provenance.Coverage
}

func (s *Service) Query(ctx context.Context, request QueryRequest) (ResultStream, error) {
	ctx, span := begin(ctx, "query", request.Language)
	if err := ctx.Err(); err != nil {
		span.End(err)
		return nil, err
	}
	q, err := normalizeRequest(request)
	if err != nil {
		span.End(err)
		return nil, err
	}
	var rows []ResultRow
	coverage := complete()
	switch q.Language {
	case SQL:
		rows, err = s.executeSQL(ctx, q)
	case JSONPointer:
		rows, err = s.executeJSON(ctx, q)
		coverage = jsonCoverage()
	default:
		err = ErrUnsupported
	}
	if err != nil {
		span.End(err)
		return nil, err
	}
	return &resultStream{ctx: ctx, span: span, rows: rows, coverage: coverage}, nil
}
func (r *resultStream) Next() (ResultRow, error) {
	if r.closed {
		return ResultRow{}, io.ErrClosedPipe
	}
	if r.err != nil {
		return ResultRow{}, r.err
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		r.span.RecordError(err)
		return ResultRow{}, err
	}
	if r.index == len(r.rows) {
		if !r.eof {
			r.eof = true
			r.span.Count("result_eof", 1)
		}
		return ResultRow{}, io.EOF
	}
	row := r.rows[r.index]
	r.index++
	data, err := json.Marshal(row)
	if err != nil {
		r.err = err
		r.span.RecordError(err)
		return ResultRow{}, err
	}
	var copy ResultRow
	if err = strictJSON(data, &copy); err != nil {
		r.err = err
		r.span.RecordError(err)
		return ResultRow{}, err
	}
	r.span.Count("rows_returned", 1)
	return copy, nil
}
func (r *resultStream) Coverage() provenance.Coverage {
	if !r.eof || r.err != nil {
		return incomplete("result not fully consumed successfully")
	}
	c := r.coverage
	c.Reasons = append([]string(nil), c.Reasons...)
	return c
}
func (r *resultStream) Close() error {
	if r.closed {
		return r.err
	}
	r.closed = true
	r.rows = nil
	if !r.eof {
		r.span.Count("early_closed", 1)
	}
	r.span.End(r.err)
	return r.err
}

var _ ResultStream = (*resultStream)(nil)
