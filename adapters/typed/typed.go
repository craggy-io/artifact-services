// Package typed is compile-time sugar over assertions and objects, for Go callers that
// would otherwise plumb json.RawMessage. It uses encoding/json and struct tags, which
// is exactly the kind of Go opinion core stays out of.
package typed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
)

// ErrUnsupportedType reports a Go type with no shape: maps, interfaces and channels
// cannot state which properties a value has, so they cannot be validated.
var ErrUnsupportedType = errors.New("typed: type has no shape")

var columnID = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ShapeOf derives a shape from a Go type by its JSON encoding. Pointers are nullable,
// and fields without omitempty are required.
func ShapeOf[T any]() (objects.Shape, error) {
	return shapeOf(reflect.TypeFor[T](), map[reflect.Type]bool{})
}

func shapeOf(t reflect.Type, visiting map[reflect.Type]bool) (objects.Shape, error) {
	if visiting[t] {
		return objects.Shape{}, fmt.Errorf("%w: recursive type %s", ErrUnsupportedType, t)
	}
	switch t.Kind() {
	case reflect.Pointer:
		inner, err := shapeOf(t.Elem(), visiting)
		inner.Nullable = true
		return inner, err
	case reflect.String:
		return objects.Shape{Type: "string"}, nil
	case reflect.Bool:
		return objects.Shape{Type: "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return objects.Shape{Type: "integer"}, nil
	case reflect.Float32, reflect.Float64:
		return objects.Shape{Type: "number"}, nil
	case reflect.Slice, reflect.Array:
		visiting[t] = true
		defer delete(visiting, t)
		items, err := shapeOf(t.Elem(), visiting)
		if err != nil {
			return objects.Shape{}, err
		}
		return objects.Shape{Type: "array", Items: &items, Nullable: t.Kind() == reflect.Slice}, nil
	case reflect.Struct:
		visiting[t] = true
		defer delete(visiting, t)
		shape := objects.Shape{Type: "object", Properties: map[string]objects.Shape{}}
		for _, field := range structFields(t) {
			child, err := shapeOf(field.Type, visiting)
			if err != nil {
				return objects.Shape{}, fmt.Errorf("field %s: %w", field.name, err)
			}
			shape.Properties[field.name] = child
			if field.required {
				shape.Required = append(shape.Required, field.name)
			}
		}
		return shape, nil
	}
	return objects.Shape{}, fmt.Errorf("%w: %s", ErrUnsupportedType, t)
}

type structField struct {
	reflect.StructField
	name     string
	required bool
}

func structFields(t reflect.Type) []structField {
	var fields []structField
	for field := range t.Fields() {
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("json")
		name, options, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields = append(fields, structField{StructField: field, name: name, required: !strings.Contains(options, "omitempty") && !strings.Contains(options, "omitzero")})
	}
	return fields
}

// KindDefinition names a kind. Shape overrides the one derived from T.
type KindDefinition struct {
	Name     string
	Version  int
	Keyed    bool
	Writers  []string
	Shape    *objects.Shape
	Register assertions.Writer
}

// Kind is a registered assertion kind whose values are T.
type Kind[T any] struct {
	service *assertions.Service
	id      assertions.KindID
}

// DefineKind registers a kind, idempotently, for value type T.
func DefineKind[T any](ctx context.Context, service *assertions.Service, definition KindDefinition) (Kind[T], error) {
	shape := definition.Shape
	if shape == nil {
		derived, err := ShapeOf[T]()
		if err != nil {
			return Kind[T]{}, err
		}
		shape = &derived
	}
	id, err := service.Register(ctx, assertions.Kind{Name: definition.Name, Version: definition.Version, Shape: *shape, Keyed: definition.Keyed, Writers: definition.Writers}, definition.Register)
	if err != nil {
		return Kind[T]{}, err
	}
	return Kind[T]{service: service, id: id}, nil
}

// ID returns the kind's identity.
func (k Kind[T]) ID() assertions.KindID { return k.id }

// Said is an assertion with its value decoded.
type Said[T any] struct {
	assertions.Assertion
	Claim T
}

func decode[T any](a assertions.Assertion) (Said[T], error) {
	said := Said[T]{Assertion: a}
	if err := json.Unmarshal(a.Value, &said.Claim); err != nil {
		return said, fmt.Errorf("typed: assertion %s: %w", a.ID, err)
	}
	return said, nil
}

// Option adjusts a claim. Options exist because most claims need few of these fields.
type Option func(*assertions.Request)

// WithID makes a claim retryable.
func WithID(id assertions.ID) Option { return func(r *assertions.Request) { r.ID = id } }

// WithKey sets the key a keyed kind requires.
func WithKey(key string) Option { return func(r *assertions.Request) { r.Key = key } }

// WithEvidence cites references.
func WithEvidence(refs ...provenance.Ref) Option {
	return func(r *assertions.Request) { r.Evidence = append(r.Evidence, refs...) }
}

// Replacing supersedes an assertion.
func Replacing(id assertions.ID) Option { return func(r *assertions.Request) { r.Replaces = id } }

// WithActivity names the recording activity.
func WithActivity(activity string) Option {
	return func(r *assertions.Request) { r.Activity = activity }
}

// WithActor names the recording actor, an unverified label.
func WithActor(actor string) Option { return func(r *assertions.Request) { r.Actor = actor } }

// WithWriter presents a credential.
func WithWriter(writer assertions.Writer) Option {
	return func(r *assertions.Request) { r.Writer = writer }
}

// Claim records value about subject.
func (k Kind[T]) Claim(ctx context.Context, subject provenance.Ref, value T, options ...Option) (assertions.ID, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("typed: encode claim: %w", err)
	}
	request := assertions.Request{Kind: k.id, Subject: subject, Value: encoded}
	for _, option := range options {
		option(&request)
	}
	return k.service.Record(ctx, request)
}

// Get returns one assertion of this kind by ID, whatever its state.
func (k Kind[T]) Get(ctx context.Context, id assertions.ID) (Said[T], error) {
	a, err := k.service.Get(ctx, id)
	if err != nil {
		return Said[T]{}, err
	}
	if a.Kind != k.id {
		return Said[T]{}, fmt.Errorf("%w: %s is kind %s, not %s", assertions.ErrNotFound, id, a.Kind, k.id)
	}
	return decode[T](a)
}

// Keyed returns the current assertion for a key.
func (k Kind[T]) Keyed(ctx context.Context, key string) (Said[T], error) {
	a, err := k.service.Current(ctx, k.id, key)
	if err != nil {
		return Said[T]{}, err
	}
	return decode[T](a)
}

func (k Kind[T]) iterate(ctx context.Context, q assertions.Query) iter.Seq2[Said[T], error] {
	return func(yield func(Said[T], error) bool) {
		q.Kind = k.id
		for {
			page, err := k.service.List(ctx, q)
			if err != nil {
				yield(Said[T]{}, err)
				return
			}
			for _, a := range page.Assertions {
				said, err := decode[T](a)
				if !yield(said, err) || err != nil {
					return
				}
			}
			if q.Cursor = page.NextCursor; q.Cursor == "" {
				return
			}
		}
	}
}

// Current iterates every current assertion of this kind.
func (k Kind[T]) Current(ctx context.Context) iter.Seq2[Said[T], error] {
	return k.iterate(ctx, assertions.Query{})
}

// About iterates current assertions of this kind about a subject.
func (k Kind[T]) About(ctx context.Context, subject provenance.Ref) iter.Seq2[Said[T], error] {
	return k.iterate(ctx, assertions.Query{Subject: &subject})
}

// Citing iterates current assertions of this kind that cite a reference as evidence.
func (k Kind[T]) Citing(ctx context.Context, cited provenance.Ref) iter.Seq2[Said[T], error] {
	return k.iterate(ctx, assertions.Query{Evidence: &cited})
}

// Services are what a typed table publishes through.
type Services struct {
	CAS       cas.Store
	Artifacts *artifacts.Service
	Objects   *objects.Service
}

// Table is a table schema whose rows are T. Exported fields become columns, named by
// their JSON tags, which must be valid column identifiers.
type Table[T any] struct {
	services   Services
	schema     artifacts.ID
	definition objects.SchemaDefinition
	TempDir    string
}

// DefineTable derives and publishes a schema from T. Key names columns by JSON name.
func DefineTable[T any](ctx context.Context, services Services, key ...string) (Table[T], error) {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		return Table[T]{}, fmt.Errorf("%w: table rows must be structs, not %s", ErrUnsupportedType, t)
	}
	definition := objects.SchemaDefinition{Version: objects.SchemaVersion, Model: objects.Table, Key: key}
	for _, field := range structFields(t) {
		if !columnID.MatchString(field.name) {
			return Table[T]{}, fmt.Errorf("typed: %q is not a valid column identifier", field.name)
		}
		shape, err := shapeOf(field.Type, map[reflect.Type]bool{})
		if err != nil {
			return Table[T]{}, fmt.Errorf("field %s: %w", field.name, err)
		}
		definition.Columns = append(definition.Columns, objects.Column{ID: field.name, Name: field.name, Shape: shape})
	}
	schema, err := services.Objects.DefineSchema(ctx, definition)
	if err != nil {
		return Table[T]{}, err
	}
	return Table[T]{services: services, schema: schema, definition: definition}, nil
}

// Schema returns the table's schema artifact.
func (t Table[T]) Schema() artifacts.ID { return t.schema }

// RowID derives a keyed row's ID from key values, for use in a reference.
func (t Table[T]) RowID(key ...any) (string, error) { return objects.KeyRowID(t.definition, key...) }

// Put streams rows through a temporary file and publishes them as a validated table.
// An unkeyed table gets row IDs from row position.
func (t Table[T]) Put(ctx context.Context, rows iter.Seq2[T, error], metadata map[string]json.RawMessage) (id artifacts.ID, result error) {
	spool, err := os.CreateTemp(t.TempDir, "typed-rows-*")
	if err != nil {
		return "", err
	}
	defer func() { result = errors.Join(result, spool.Close(), os.Remove(spool.Name())) }()
	encoder := json.NewEncoder(spool)
	index := 0
	for row, err := range rows {
		if err != nil {
			return "", err
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		encoded, err := encodeRow(row, t.definition, index)
		if err != nil {
			return "", err
		}
		if err := encoder.Encode(encoded); err != nil {
			return "", err
		}
		index++
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	put, err := t.services.CAS.Put(ctx, spool)
	if err != nil {
		return "", err
	}
	raw, err := t.services.Artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "rows.ndjson", Content: put.ID, Size: put.Size}}})
	if err != nil {
		return "", err
	}
	return t.services.Objects.Publish(ctx, objects.PublishRequest{ValidationRequest: objects.ValidationRequest{Data: raw, Schema: t.schema, Encoding: objects.NDJSON}, Metadata: metadata})
}

func encodeRow[T any](row T, definition objects.SchemaDefinition, index int) (objects.Row, error) {
	data, err := json.Marshal(row)
	if err != nil {
		return objects.Row{}, fmt.Errorf("typed: encode row %d: %w", index, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var values map[string]any
	if err := decoder.Decode(&values); err != nil {
		return objects.Row{}, err
	}
	encoded := objects.Row{Values: values}
	if len(definition.Key) == 0 {
		encoded.ID = fmt.Sprintf("row-%d", index)
	}
	return encoded, nil
}

// Slice adapts rows held in memory to Put.
func Slice[T any](rows []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

// Rows iterates a table artifact, decoding each row into T. Bounded memory and
// verification at EOF are as for objects.Service.Rows: a corrupt table yields its
// error only after its rows, so stop on the first error and do not trust rows from an
// iteration that ended with one.
func Rows[T any](ctx context.Context, service *objects.Service, id artifacts.ID) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		stream, err := service.Rows(ctx, id)
		if err != nil {
			yield(zero, err)
			return
		}
		closed := false
		defer func() {
			if !closed {
				_ = stream.Close()
			}
		}()
		for {
			row, err := stream.Next()
			if err == io.EOF {
				closed = true
				if err := stream.Close(); err != nil {
					yield(zero, err)
				}
				return
			}
			if err != nil {
				yield(zero, err)
				return
			}
			encoded, err := json.Marshal(row.Values)
			if err != nil {
				yield(zero, err)
				return
			}
			var value T
			if err := json.Unmarshal(encoded, &value); err != nil {
				yield(zero, err)
				return
			}
			if !yield(value, nil) {
				return
			}
		}
	}
}
