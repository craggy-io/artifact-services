package objects

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type token struct{ kind, text string }
type expression struct {
	kind, text  string
	value       any
	left, right *expression
}
type projection struct {
	expression *expression
	alias      string
}
type relation struct {
	input, alias string
	on           *expression
}
type sqlPlan struct {
	selects []projection
	from    relation
	joins   []relation
	where   *expression
	groups  []*expression
}

func lexSQL(text string) ([]token, error) {
	if len(text) > 65536 {
		return nil, ErrLimit
	}
	var out []token
	for i := 0; i < len(text); {
		c := text[i]
		if unicode.IsSpace(rune(c)) {
			i++
			continue
		}
		start := i
		switch {
		case c == '\'':
			i++
			var b strings.Builder
			closed := false
			for i < len(text) {
				if text[i] == '\'' {
					if i+1 < len(text) && text[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				b.WriteByte(text[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("%w: unterminated string", ErrQuery)
			}
			out = append(out, token{"string", b.String()})
		case c == ':':
			i++
			for i < len(text) && (text[i] == '_' || text[i] >= 'a' && text[i] <= 'z' || text[i] >= 'A' && text[i] <= 'Z' || text[i] >= '0' && text[i] <= '9') {
				i++
			}
			if i == start+1 {
				return nil, ErrQuery
			}
			out = append(out, token{"param", text[start+1 : i]})
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			i++
			for i < len(text) && (text[i] == '_' || text[i] >= 'a' && text[i] <= 'z' || text[i] >= 'A' && text[i] <= 'Z' || text[i] >= '0' && text[i] <= '9') {
				i++
			}
			out = append(out, token{"ident", text[start:i]})
		case c >= '0' && c <= '9':
			i++
			for i < len(text) && (text[i] >= '0' && text[i] <= '9' || text[i] == '.') {
				i++
			}
			if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
				i++
				if i < len(text) && (text[i] == '+' || text[i] == '-') {
					i++
				}
				for i < len(text) && text[i] >= '0' && text[i] <= '9' {
					i++
				}
			}
			n := text[start:i]
			if _, err := strconv.ParseFloat(n, 64); err != nil {
				return nil, ErrQuery
			}
			out = append(out, token{"number", n})
		case strings.ContainsRune("(),.+-*/=<>!;", rune(c)):
			i++
			if i < len(text) && ((c == '<' || c == '>' || c == '!') && text[i] == '=' || c == '<' && text[i] == '>') {
				i++
			}
			out = append(out, token{"symbol", text[start:i]})
		default:
			return nil, fmt.Errorf("%w: token %q", ErrUnsupported, c)
		}
		if len(out) > 8192 {
			return nil, ErrLimit
		}
	}
	return out, nil
}

type parser struct {
	tokens    []token
	at, depth int
	err       error
}

func (p *parser) peek() token {
	if p.at >= len(p.tokens) {
		return token{}
	}
	return p.tokens[p.at]
}
func (p *parser) take() token {
	t := p.peek()
	if p.at < len(p.tokens) {
		p.at++
	}
	return t
}
func (p *parser) accept(s string) bool {
	if strings.EqualFold(p.peek().text, s) {
		p.at++
		return true
	}
	return false
}
func (p *parser) expect(s string) {
	if !p.accept(s) && p.err == nil {
		p.err = fmt.Errorf("%w: expected %s", ErrQuery, s)
	}
}
func (p *parser) name() string {
	t := p.take()
	if t.kind != "ident" {
		p.err = fmt.Errorf("%w: identifier required", ErrQuery)
	}
	return strings.ToLower(t.text)
}

// rawName preserves identifier case; output aliases name schema column IDs,
// which are case-sensitive, unlike display-name matching.
func (p *parser) rawName() string {
	t := p.take()
	if t.kind != "ident" {
		p.err = fmt.Errorf("%w: identifier required", ErrQuery)
	}
	return t.text
}
func (p *parser) expr(min int) *expression {
	p.depth++
	defer func() { p.depth-- }()
	if p.depth > 64 {
		p.err = ErrLimit
		return &expression{kind: "literal"}
	}
	t := p.take()
	var e *expression
	switch {
	case t.kind == "number":
		e = &expression{kind: "literal", value: json.Number(t.text), text: t.text}
	case t.kind == "string":
		e = &expression{kind: "literal", value: t.text, text: "'" + strings.ReplaceAll(t.text, "'", "''") + "'"}
	case t.kind == "param":
		e = &expression{kind: "param", text: t.text}
	case t.text == "-" || t.text == "+":
		e = &expression{kind: "unary", text: t.text, left: p.expr(30)}
	case t.text == "(":
		e = p.expr(0)
		p.expect(")")
	case t.kind == "ident":
		name := strings.ToLower(t.text)
		if p.accept("(") {
			if name != "sum" && name != "count" {
				p.err = fmt.Errorf("%w: function %s", ErrUnsupported, name)
			}
			var arg *expression
			if p.accept("*") {
				arg = &expression{kind: "star"}
			} else {
				arg = p.expr(0)
			}
			p.expect(")")
			e = &expression{kind: "aggregate", text: name, left: arg}
		} else if name == "null" {
			e = &expression{kind: "literal", text: "NULL"}
		} else if name == "true" || name == "false" {
			e = &expression{kind: "literal", text: name, value: name == "true"}
		} else {
			if p.accept(".") {
				name += "." + p.name()
			}
			e = &expression{kind: "column", text: name}
		}
	default:
		p.err = fmt.Errorf("%w: expression near %q", ErrUnsupported, t.text)
		e = &expression{kind: "literal"}
	}
	for p.err == nil {
		op := p.peek().text
		prec := 0
		switch op {
		case "+", "-":
			prec = 10
		case "*", "/":
			prec = 20
		}
		if prec == 0 || prec < min {
			break
		}
		p.take()
		e = &expression{kind: "binary", text: op, left: e, right: p.expr(prec + 1)}
	}
	return e
}
func (p *parser) condition() *expression {
	left := p.comparison()
	for p.accept("AND") {
		left = &expression{kind: "binary", text: "AND", left: left, right: p.comparison()}
	}
	return left
}
func (p *parser) comparison() *expression {
	left := p.expr(0)
	op := p.take().text
	switch op {
	case "=", "!=", "<>", "<", ">", "<=", ">=":
	default:
		p.err = fmt.Errorf("%w: comparison operator %q", ErrUnsupported, op)
	}
	return &expression{kind: "binary", text: op, left: left, right: p.expr(0)}
}
func (p *parser) relation() relation {
	r := relation{input: p.name()}
	r.alias = r.input
	if p.accept("AS") {
		r.alias = p.name()
	}
	return r
}
func parseSQL(text string) (sqlPlan, error) {
	tokens, err := lexSQL(text)
	if err != nil {
		return sqlPlan{}, err
	}
	p := &parser{tokens: tokens}
	plan := sqlPlan{}
	p.expect("SELECT")
	names := map[string]bool{}
	for p.err == nil {
		e := p.expr(0)
		alias := ""
		if p.accept("AS") {
			alias = p.rawName()
		} else if e.kind == "column" {
			parts := strings.Split(e.text, ".")
			alias = parts[len(parts)-1]
		} else {
			p.err = fmt.Errorf("%w: expressions require AS output_id", ErrQuery)
		}
		if !identifier.MatchString(alias) || names[alias] {
			p.err = fmt.Errorf("%w: duplicate/invalid output identity", ErrQuery)
		}
		names[alias] = true
		plan.selects = append(plan.selects, projection{e, alias})
		if len(plan.selects) > 256 {
			p.err = ErrLimit
		}
		if !p.accept(",") {
			break
		}
	}
	p.expect("FROM")
	plan.from = p.relation()
	aliases := map[string]bool{plan.from.alias: true}
	for p.accept("INNER") || strings.EqualFold(p.peek().text, "JOIN") {
		p.expect("JOIN")
		r := p.relation()
		if aliases[r.alias] {
			p.err = fmt.Errorf("%w: duplicate relation alias", ErrQuery)
		}
		aliases[r.alias] = true
		p.expect("ON")
		r.on = p.condition()
		if !joinEquality(r.on) {
			p.err = fmt.Errorf("%w: JOIN requires column equality predicates", ErrUnsupported)
		}
		plan.joins = append(plan.joins, r)
		if len(plan.joins) > 7 {
			p.err = ErrLimit
		}
	}
	if p.accept("WHERE") {
		plan.where = p.condition()
	}
	if p.accept("GROUP") {
		p.expect("BY")
		for {
			plan.groups = append(plan.groups, p.expr(0))
			if !p.accept(",") {
				break
			}
		}
	}
	p.accept(";")
	if p.at != len(p.tokens) && p.err == nil {
		p.err = fmt.Errorf("%w: trailing SQL clause %q", ErrUnsupported, p.peek().text)
	}
	if p.err != nil {
		return plan, p.err
	}
	for _, g := range plan.groups {
		if hasAggregate(g) {
			return plan, fmt.Errorf("%w: aggregate GROUP BY", ErrUnsupported)
		}
	}
	if hasAggregate(plan.where) {
		return plan, fmt.Errorf("%w: aggregate WHERE", ErrUnsupported)
	}
	aggregated := len(plan.groups) > 0
	for _, s := range plan.selects {
		aggregated = aggregated || hasAggregate(s.expression)
		if nestedAggregate(s.expression, false) {
			return plan, fmt.Errorf("%w: nested aggregate", ErrUnsupported)
		}
	}
	if aggregated {
		for _, s := range plan.selects {
			if !groupedExpression(s.expression, plan.groups) {
				return plan, fmt.Errorf("%w: nonaggregate columns must occur in GROUP BY", ErrUnsupported)
			}
		}
	}

	return plan, nil
}
func joinEquality(e *expression) bool {
	if e == nil {
		return false
	}
	if e.text == "AND" {
		return joinEquality(e.left) && joinEquality(e.right)
	}
	return e.text == "=" && e.left.kind == "column" && e.right.kind == "column"
}
func hasAggregate(e *expression) bool {
	return e != nil && (e.kind == "aggregate" || hasAggregate(e.left) || hasAggregate(e.right))
}
func nestedAggregate(e *expression, inside bool) bool {
	if e == nil {
		return false
	}
	if e.kind == "aggregate" && inside {
		return true
	}
	return nestedAggregate(e.left, inside || e.kind == "aggregate") || nestedAggregate(e.right, inside || e.kind == "aggregate")
}
func expressionText(e *expression) string {
	if e == nil {
		return ""
	}
	switch e.kind {
	case "binary":
		return "(" + expressionText(e.left) + " " + e.text + " " + expressionText(e.right) + ")"
	case "unary":
		return e.text + expressionText(e.left)
	case "aggregate":
		return strings.ToUpper(e.text) + "(" + expressionText(e.left) + ")"
	case "param":
		return ":" + e.text
	case "star":
		return "*"
	}
	return e.text
}

// sameColumnRef compares column references by identity, tolerating one side
// being written unqualified (parser lowercases column text).
func sameColumnRef(a, b string) bool {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	if pa[len(pa)-1] != pb[len(pb)-1] {
		return false
	}
	if len(pa) == 2 && len(pb) == 2 {
		return pa[0] == pb[0]
	}
	return true
}

func groupedExpression(e *expression, groups []*expression) bool {
	if e == nil || e.kind == "aggregate" || e.kind == "literal" || e.kind == "param" {
		return true
	}
	for _, g := range groups {
		if e.kind == "column" && g.kind == "column" {
			if sameColumnRef(e.text, g.text) {
				return true
			}
			continue
		}
		if expressionText(e) == expressionText(g) {
			return true
		}
	}
	if e.kind == "column" {
		return false
	}
	return groupedExpression(e.left, groups) && groupedExpression(e.right, groups)
}
