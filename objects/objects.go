// Package objects assigns validated runtime types to immutable artifacts and
// executes bounded read-only content queries with explicit provenance evidence.
package objects

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/provenance"
)

const (
	SchemaVersion    = "objects-schema-v1"
	ValidatorVersion = "objects-validator-v1"
	Table            = "table"
	JSON             = "json"
	NDJSON           = "ndjson-rows-v1"
	JSONEncoding     = "json-v1"
	SQL              = "sql-v1"
	JSONPointer      = "json-pointer-v1"
)

var (
	ErrSchema      = errors.New("objects: invalid schema")
	ErrValidation  = errors.New("objects: validation failed")
	ErrUnsupported = errors.New("objects: unsupported operation")
	ErrLimit       = errors.New("objects: resource limit")
	ErrQuery       = errors.New("objects: invalid query")
)

type Shape struct {
	Type       string            `json:"type"`
	Nullable   bool              `json:"nullable,omitempty"`
	Properties map[string]Shape  `json:"properties,omitempty"`
	Required   []string          `json:"required,omitempty"`
	Items      *Shape            `json:"items,omitempty"`
	Minimum    *float64          `json:"minimum,omitempty"`
	Maximum    *float64          `json:"maximum,omitempty"`
	MaxLength  int               `json:"max_length,omitempty"`
	Enum       []json.RawMessage `json:"enum,omitempty"`
}
type Column struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Shape  Shape  `json:"shape"`
	Unique bool   `json:"unique,omitempty"`
}

// SchemaDefinition describes a table or JSON document. Key names the column IDs that
// identify a table row: row IDs are then derived from those values (see KeyRowID),
// are optional in the data, and must match if present. Key columns must be
// non-nullable strings, integers or booleans, whose canonical forms are exact.
type SchemaDefinition struct {
	Version string   `json:"version"`
	Model   string   `json:"model"`
	Columns []Column `json:"columns,omitempty"`
	JSON    *Shape   `json:"json,omitempty"`
	Key     []string `json:"key,omitempty"`
}

// Row values use stable column IDs, not mutable display names or offsets.
type Row struct {
	ID     string         `json:"id,omitempty"`
	Values map[string]any `json:"values"`
}

// RowStream yields rows in stored order. Next returns io.EOF at the end, which is
// also when the content digest is verified: rows returned before EOF are not yet
// proven. Callers must Close it.
type RowStream interface {
	Next() (Row, error)
	Close() error
}
type ValidationRequest struct {
	Data     artifacts.ID
	Schema   artifacts.ID
	Encoding string
}
type ValidationReport struct {
	Valid               bool
	Data, Schema        artifacts.ID
	Validator, Encoding string
	Rows                int64
	Coverage            provenance.Coverage
	Issues              []string
}
type PublishRequest struct {
	ValidationRequest
	Metadata map[string]json.RawMessage
}
type Input struct {
	Alias    string
	Artifact artifacts.ID
	Revision string
}
type QueryRequest struct {
	Language, Text  string
	Inputs          []Input
	Parameters      map[string]any
	RequireComplete bool
}
type ResultRow struct {
	ID           string                             `json:"id"`
	Values       map[string]any                     `json:"values"`
	Dependencies []provenance.Dependency            `json:"dependencies"`
	Columns      map[string][]provenance.Dependency `json:"columns"`
	Expressions  map[string]string                  `json:"expressions"`
}
type ResultStream interface {
	Next() (ResultRow, error)
	Close() error
	Coverage() provenance.Coverage
}
type MaterializeRequest struct {
	Query          QueryRequest
	Schema         artifacts.ID
	OutputRevision string
	Metadata       map[string]json.RawMessage
}
type Materialized struct {
	Artifact, Evidence                     artifacts.ID
	ArtifactRegistered, EvidenceRegistered bool
	EvidencePath                           string
	Rows                                   int64
	Coverage                               provenance.Coverage
}
type Capabilities struct {
	Validator                   string
	Languages                   []string
	Types                       []string
	Operators                   []string
	TableCoverage, JSONCoverage provenance.Coverage
	Limits                      Options
}
type Options struct {
	MaxRows         int
	MaxBytes        int64
	MaxRowBytes     int
	MaxResultRows   int
	MaxJoinPairs    int
	MaxGroups       int
	MaxDependencies int
	TempDir         string
}
type Service struct {
	blobs     cas.Store
	artifacts *artifacts.Service
	limits    Options
}

func New(blobs cas.Store, store *artifacts.Service, options Options) (*Service, error) {
	if blobs == nil || store == nil {
		return nil, errors.New("objects: nil dependency")
	}
	if options.MaxRows == 0 {
		options.MaxRows = 100000
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = 64 << 20
	}
	if options.MaxRowBytes == 0 {
		options.MaxRowBytes = 1 << 20
	}
	if options.MaxResultRows == 0 {
		options.MaxResultRows = 10000
	}
	if options.MaxJoinPairs == 0 {
		options.MaxJoinPairs = 1000000
	}
	if options.MaxGroups == 0 {
		options.MaxGroups = 10000
	}
	if options.MaxDependencies == 0 {
		options.MaxDependencies = 1000000
	}
	if options.MaxRows < 1 || options.MaxBytes < 1 || options.MaxRowBytes < 1 || options.MaxResultRows < 1 || options.MaxJoinPairs < 1 || options.MaxGroups < 1 || options.MaxDependencies < 1 {
		return nil, ErrLimit
	}
	// Keep headroom for the +1 probes used by bounded readers so a limit at
	// math.MaxInt64 cannot wrap a LimitReader length negative.
	if options.MaxBytes > (1<<62) || options.MaxRowBytes > (1<<62) {
		return nil, ErrLimit
	}
	return &Service{blobs: blobs, artifacts: store, limits: options}, nil
}
func complete() provenance.Coverage {
	return provenance.Coverage{Rows: provenance.Complete, Columns: provenance.Complete, Values: provenance.Complete, Predicates: provenance.Complete}
}
func incomplete(reason string) provenance.Coverage {
	return provenance.Coverage{Rows: provenance.Incomplete, Columns: provenance.Incomplete, Values: provenance.Incomplete, Predicates: provenance.Incomplete, Reasons: []string{reason}}
}
func jsonCoverage() provenance.Coverage {
	return provenance.Coverage{Rows: provenance.Unsupported, Columns: provenance.Unsupported, Values: provenance.Complete, Predicates: provenance.Complete, Reasons: []string{"JSON Pointer reports document dependency, not table row/column mapping"}}
}
func (s *Service) Capabilities(ctx context.Context) (capabilities Capabilities, result error) {
	ctx, span := begin(ctx, "capabilities", "")
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return Capabilities{}, err
	}
	return Capabilities{Validator: ValidatorVersion, Languages: []string{SQL, JSONPointer}, Types: []string{"string", "integer", "number", "boolean", "object", "array"}, Operators: []string{"SELECT", "INNER JOIN ON equality", "WHERE comparison AND", "arithmetic + - * /", "GROUP BY", "COUNT", "SUM"}, TableCoverage: complete(), JSONCoverage: jsonCoverage(), Limits: s.limits}, nil
}
