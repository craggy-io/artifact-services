package objects

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/typedclaim"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
	"io"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func begin(ctx context.Context, op, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "objects", Operation: op}, key)
}
func strictJSON(data []byte, target any) error {
	if _, err := canon.Normalize(data); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("objects: trailing JSON")
	}
	return nil
}
func (s *Service) DefineSchema(ctx context.Context, definition SchemaDefinition) (id artifacts.ID, result error) {
	ctx, span := begin(ctx, "define_schema", "")
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateDefinition(definition); err != nil {
		return "", err
	}
	data, err := canon.Marshal(definition)
	if err != nil {
		return "", err
	}
	if len(data) > 64<<10 {
		return "", ErrLimit
	}
	return s.publishFile(ctx, "schema.json", bytes.NewReader(data), nil)
}
func (s *Service) GetSchema(ctx context.Context, id artifacts.ID) (definition SchemaDefinition, result error) {
	ctx, span := begin(ctx, "get_schema", string(id))
	defer func() { span.End(result) }()
	data, err := s.readFile(observability.WithInternal(ctx), id, "schema.json", 64<<10)
	if err != nil {
		return definition, err
	}
	if err = strictJSON(data, &definition); err != nil {
		return definition, fmt.Errorf("%w: %v", ErrSchema, err)
	}
	return definition, validateDefinition(definition)
}
func validateDefinition(d SchemaDefinition) error {
	if d.Version != SchemaVersion {
		return fmt.Errorf("%w: definition version", ErrSchema)
	}
	count := 0
	switch d.Model {
	case Table:
		if d.JSON != nil || len(d.Columns) == 0 || len(d.Columns) > 256 {
			return fmt.Errorf("%w: table columns", ErrSchema)
		}
		ids, names := map[string]bool{}, map[string]bool{}
		for _, c := range d.Columns {
			if !identifier.MatchString(c.ID) || !identifier.MatchString(c.Name) || len(c.ID) > 128 || len(c.Name) > 128 || ids[c.ID] || names[strings.ToLower(c.Name)] {
				return fmt.Errorf("%w: duplicate/invalid column identity or name", ErrSchema)
			}
			ids[c.ID] = true
			names[strings.ToLower(c.Name)] = true
			if err := validateShape(c.Shape, 0, &count); err != nil {
				return err
			}
		}
	case JSON:
		if len(d.Columns) != 0 || d.JSON == nil {
			return fmt.Errorf("%w: JSON shape", ErrSchema)
		}
		return validateShape(*d.JSON, 0, &count)
	default:
		return fmt.Errorf("%w: data model", ErrSchema)
	}
	return nil
}
func validateShape(sh Shape, depth int, count *int) error {
	*count++
	if depth > 32 || *count > 2048 || len(sh.Properties) > 256 || len(sh.Enum) > 256 || sh.MaxLength < 0 {
		return fmt.Errorf("%w: shape limits", ErrSchema)
	}
	switch sh.Type {
	case "string", "integer", "number", "boolean", "object", "array":
	default:
		return fmt.Errorf("%w: unsupported type %q", ErrSchema, sh.Type)
	}
	if sh.Type != "object" && (len(sh.Properties) > 0 || len(sh.Required) > 0) {
		return fmt.Errorf("%w: properties require object", ErrSchema)
	}
	if sh.Type == "array" {
		if sh.Items == nil {
			return fmt.Errorf("%w: array items required", ErrSchema)
		}
	} else if sh.Items != nil {
		return fmt.Errorf("%w: items require array", ErrSchema)
	}
	if sh.Minimum != nil || sh.Maximum != nil {
		if sh.Type != "integer" && sh.Type != "number" {
			return fmt.Errorf("%w: numeric constraints", ErrSchema)
		}
		for _, v := range []*float64{sh.Minimum, sh.Maximum} {
			if v != nil && (math.IsInf(*v, 0) || math.IsNaN(*v)) {
				return fmt.Errorf("%w: finite numeric bounds required", ErrSchema)
			}
		}
		if sh.Minimum != nil && sh.Maximum != nil && *sh.Minimum > *sh.Maximum {
			return fmt.Errorf("%w: inverted bounds", ErrSchema)
		}
	}
	if sh.MaxLength > 0 && sh.Type != "string" && sh.Type != "array" {
		return fmt.Errorf("%w: length constraint", ErrSchema)
	}
	required := map[string]bool{}
	for _, name := range sh.Required {
		if _, ok := sh.Properties[name]; !ok || required[name] {
			return fmt.Errorf("%w: required property", ErrSchema)
		}
		required[name] = true
	}
	for name, child := range sh.Properties {
		if name == "" || len(name) > 256 {
			return fmt.Errorf("%w: property name", ErrSchema)
		}
		if err := validateShape(child, depth+1, count); err != nil {
			return err
		}
	}
	if sh.Items != nil {
		if err := validateShape(*sh.Items, depth+1, count); err != nil {
			return err
		}
	}
	for _, raw := range sh.Enum {
		var value any
		if err := strictJSON(raw, &value); err != nil {
			return fmt.Errorf("%w: enum JSON", ErrSchema)
		}
		without := sh
		without.Enum = nil
		if err := checkValue(value, without, "enum"); err != nil {
			return fmt.Errorf("%w: %v", ErrSchema, err)
		}
	}
	return nil
}
func numeric(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, e := strconv.ParseFloat(string(n), 64)
		return f, e == nil && !math.IsInf(f, 0) && !math.IsNaN(f)
	case float64:
		return n, !math.IsInf(n, 0) && !math.IsNaN(n)
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}
func checkValue(value any, sh Shape, path string) error {
	fail := func(message string) error { return fmt.Errorf("%w: %s: %s", ErrValidation, path, message) }
	if value == nil {
		if sh.Nullable {
			return nil
		}
		return fail("null not allowed")
	}
	switch sh.Type {
	case "string":
		v, ok := value.(string)
		if !ok {
			return fail("expected string")
		}
		if sh.MaxLength > 0 && len([]rune(v)) > sh.MaxLength {
			return fail("string length")
		}
	case "integer":
		n, ok := value.(json.Number)
		if !ok {
			return fail("expected integer")
		}
		if _, err := strconv.ParseInt(string(n), 10, 64); err != nil {
			return fail("expected signed 64-bit integer")
		}
	case "number":
		if _, ok := numeric(value); !ok {
			return fail("expected finite number")
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fail("expected boolean")
		}
	case "object":
		m, ok := value.(map[string]any)
		if !ok {
			return fail("expected object")
		}
		for _, name := range sh.Required {
			if _, ok := m[name]; !ok {
				return fail("missing " + name)
			}
		}
		for name, v := range m {
			child, ok := sh.Properties[name]
			if !ok {
				return fail("unknown property " + name)
			}
			if err := checkValue(v, child, path+"."+name); err != nil {
				return err
			}
		}
	case "array":
		a, ok := value.([]any)
		if !ok {
			return fail("expected array")
		}
		if sh.MaxLength > 0 && len(a) > sh.MaxLength {
			return fail("array length")
		}
		for i, v := range a {
			if err := checkValue(v, *sh.Items, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	if sh.Minimum != nil || sh.Maximum != nil {
		n, ok := rat(value)
		if !ok {
			return fail("number")
		}
		if sh.Minimum != nil && n.Cmp(new(big.Rat).SetFloat64(*sh.Minimum)) < 0 || sh.Maximum != nil && n.Cmp(new(big.Rat).SetFloat64(*sh.Maximum)) > 0 {
			return fail("numeric bound")
		}
	}
	if len(sh.Enum) > 0 {
		encoded, err := canon.Marshal(value)
		if err != nil {
			return err
		}
		match := false
		for _, raw := range sh.Enum {
			candidate, err := canon.Normalize(raw)
			if err != nil {
				return err
			}
			if bytes.Equal(encoded, candidate) {
				match = true
				break
			}
		}
		if !match {
			return fail("enum constraint")
		}
	}
	return nil
}
func (s *Service) Validate(ctx context.Context, request ValidationRequest) (report ValidationReport, result error) {
	ctx, span := begin(ctx, "validate", string(request.Data))
	defer func() {
		if result != nil {
			report.Issues = []string{result.Error()}
			report.Coverage = incomplete(result.Error())
		}
		span.End(result)
	}()
	report = ValidationReport{Data: request.Data, Schema: request.Schema, Encoding: request.Encoding, Validator: ValidatorVersion, Coverage: incomplete("not completed")}
	definition, err := s.GetSchema(observability.WithInternal(ctx), request.Schema)
	if err != nil {
		return report, err
	}
	manifest, err := s.artifacts.Get(observability.WithInternal(ctx), request.Data)
	if err != nil {
		return report, err
	}
	if len(manifest.Entries) != 1 {
		return report, fmt.Errorf("%w: one data entry required", ErrValidation)
	}
	switch definition.Model {
	case Table:
		if request.Encoding != NDJSON {
			return report, ErrUnsupported
		}
		err = s.rows(ctx, request.Data, manifest.Entries[0].Path, definition, func(Row) error { report.Rows++; span.Count("rows_validated", 1); return nil })
	case JSON:
		if request.Encoding != JSONEncoding {
			return report, ErrUnsupported
		}
		var v any
		data, e := s.readFile(observability.WithInternal(ctx), request.Data, manifest.Entries[0].Path, s.limits.MaxBytes)
		if e != nil {
			return report, e
		}
		if e = strictJSON(data, &v); e != nil {
			return report, fmt.Errorf("%w: %v", ErrValidation, e)
		}
		err = checkValue(v, *definition.JSON, "$")
		report.Rows = 1
	}
	if err != nil {
		return report, err
	}
	report.Valid = true
	report.Coverage = complete()
	span.Count("validated", 1)
	return report, nil
}
func (s *Service) Publish(ctx context.Context, request PublishRequest) (id artifacts.ID, result error) {
	ctx, span := begin(ctx, "publish", string(request.Data))
	defer func() { span.End(result) }()
	report, err := s.Validate(observability.WithInternal(ctx), request.ValidationRequest)
	if err != nil || !report.Valid {
		return "", err
	}
	m, err := s.artifacts.Get(observability.WithInternal(ctx), request.Data)
	if err != nil {
		return "", err
	}
	m.Metadata = request.Metadata
	id, err = s.artifacts.PublishValidated(ctx, m, typedclaim.New(string(request.Schema), string(request.Data), ValidatorVersion, request.Encoding))
	if err == nil {
		span.Count("typed_published", 1)
	}
	return id, err
}

// PublicationError preserves support-content and artifact stages on failures.
// A known content/manifest ID is not by itself evidence of commitment.
type PublicationError struct {
	Content            cas.ID
	ContentCommitted   bool
	Artifact           artifacts.ID
	ArtifactRegistered bool
	Err                error
}

func (e *PublicationError) Error() string {
	return fmt.Sprintf("objects: file publication content=%s committed=%t artifact=%s registered=%t: %v", e.Content, e.ContentCommitted, e.Artifact, e.ArtifactRegistered, e.Err)
}
func (e *PublicationError) Unwrap() error { return e.Err }
func registered(id artifacts.ID, err error) bool {
	if err == nil {
		return id != ""
	}
	var artifactErr *artifacts.PublishError
	if errors.As(err, &artifactErr) {
		return artifactErr.Registered
	}
	var publicationErr *PublicationError
	return errors.As(err, &publicationErr) && publicationErr.ArtifactRegistered
}
func (s *Service) publishFile(ctx context.Context, path string, r io.Reader, metadata map[string]json.RawMessage) (artifacts.ID, error) {
	put, err := s.blobs.Put(observability.WithInternal(ctx), r)
	if err != nil {
		var outcome *cas.PutError
		committed := errors.As(err, &outcome) && outcome.Committed
		return "", &PublicationError{Content: put.ID, ContentCommitted: committed, Err: err}
	}
	id, err := s.artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: path, Content: put.ID, Size: put.Size}}, Metadata: metadata})
	if err != nil {
		return id, &PublicationError{Content: put.ID, ContentCommitted: true, Artifact: id, ArtifactRegistered: registered(id, err), Err: err}
	}
	return id, nil
}

func (s *Service) readFile(ctx context.Context, id artifacts.ID, path string, max int64) (data []byte, result error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := s.artifacts.OpenFile(ctx, id, path)
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, r.Close()) }()
	data, err = io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}
func (s *Service) rows(ctx context.Context, id artifacts.ID, path string, d SchemaDefinition, visit func(Row) error) (result error) {
	r, err := s.artifacts.OpenFile(observability.WithInternal(ctx), id, path)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, r.Close()) }()
	scanner := bufio.NewScanner(io.LimitReader(r, s.limits.MaxBytes+1))
	scanner.Buffer(make([]byte, 4096), s.limits.MaxRowBytes)
	seen := map[string]bool{}
	unique := map[string]map[string]bool{}
	for _, c := range d.Columns {
		if c.Unique {
			unique[c.ID] = map[string]bool{}
		}
	}
	total := int64(0)
	count := 0
	for scanner.Scan() {
		if err = ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		total += int64(len(line)) + 1
		if total > s.limits.MaxBytes {
			return ErrLimit
		}
		count++
		if count > s.limits.MaxRows {
			return ErrLimit
		}
		var row Row
		if err = strictJSON(line, &row); err != nil {
			return fmt.Errorf("%w: row JSON: %v", ErrValidation, err)
		}
		if row.ID == "" || len(row.ID) > 256 || seen[row.ID] {
			return fmt.Errorf("%w: invalid/duplicate row ID", ErrValidation)
		}
		seen[row.ID] = true
		if len(row.Values) != len(d.Columns) {
			return fmt.Errorf("%w: row column count", ErrValidation)
		}
		for _, c := range d.Columns {
			v, ok := row.Values[c.ID]
			if !ok {
				return fmt.Errorf("%w: missing column %s", ErrValidation, c.ID)
			}
			if err = checkValue(v, c.Shape, "row "+row.ID+"."+c.ID); err != nil {
				return err
			}
			if c.Unique {
				b, e := canon.Marshal(v)
				if e != nil {
					return e
				}
				key := string(b)
				if unique[c.ID][key] {
					return fmt.Errorf("%w: uniqueness %s", ErrValidation, c.ID)
				}
				unique[c.ID][key] = true
			}
		}
		if err = visit(row); err != nil {
			return err
		}
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("%w: row read: %w", ErrLimit, err)
	}
	return ctx.Err()
}
func (s *Service) typed(ctx context.Context, id artifacts.ID) (SchemaDefinition, artifacts.ID, artifacts.Manifest, string, error) {
	claim, err := s.artifacts.ValidatedClaim(observability.WithInternal(ctx), id)
	if err != nil {
		return SchemaDefinition{}, "", artifacts.Manifest{}, "", err
	}
	schema, _, validator, encoding, valid := claim.Fields()
	if !valid || validator != ValidatorVersion {
		return SchemaDefinition{}, "", artifacts.Manifest{}, "", ErrUnsupported
	}
	d, err := s.GetSchema(observability.WithInternal(ctx), artifacts.ID(schema))
	if err != nil {
		return d, "", artifacts.Manifest{}, "", err
	}
	m, err := s.artifacts.Get(observability.WithInternal(ctx), id)
	if err != nil {
		return d, "", m, "", err
	}
	if len(m.Entries) != 1 {
		return d, "", m, "", ErrValidation
	}
	return d, artifacts.ID(schema), m, encoding, nil
}
func (s *Service) ValidateRef(ctx context.Context, ref provenance.Ref) (result error) {
	ctx, span := begin(ctx, "validate_ref", ref.Artifact)
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	d, schema, m, encoding, err := s.typed(ctx, artifacts.ID(ref.Artifact))
	if err != nil {
		return err
	}
	if !ref.Detailed() {
		return nil
	}
	if d.Model != Table || encoding != NDJSON {
		return ErrUnsupported
	}
	if ref.Schema != "" && ref.Schema != string(schema) {
		return fmt.Errorf("%w: schema reference mismatch", ErrValidation)
	}
	if ref.Column != "" {
		found := false
		for _, c := range d.Columns {
			if c.ID == ref.Column {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("%w: unknown column identity", ErrValidation)
		}
	}
	if ref.Row != "" {
		found := false
		err = s.rows(ctx, artifacts.ID(ref.Artifact), m.Entries[0].Path, d, func(row Row) error {
			if row.ID == ref.Row {
				found = true
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: unknown row identity", ErrValidation)
		}
	}
	return nil
}
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
