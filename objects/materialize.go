package objects

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
	"io"
	"os"
)

const viewVersion = "objects-view-v1"

type viewDefinition struct {
	Version string       `json:"version"`
	Query   QueryRequest `json:"query"`
}

// MaterializeError retains the result stage. Artifact can be committed even if
// subsequent evidence publication/cleanup fails; no lineage records are created.
type MaterializeError struct {
	Materialized
	Err error
}

func (e *MaterializeError) Error() string {
	return fmt.Sprintf("objects: materialize artifact=%s evidence=%s: %v", e.Artifact, e.Evidence, e.Err)
}
func (e *MaterializeError) Unwrap() error { return e.Err }
func (s *Service) validateView(ctx context.Context, q QueryRequest) (QueryRequest, error) {
	q, err := normalizeRequest(q)
	if err != nil {
		return q, err
	}
	switch q.Language {
	case SQL:
		if _, err = parseSQL(q.Text); err != nil {
			return q, err
		}
	case JSONPointer:
		if len(q.Inputs) != 1 || q.RequireComplete {
			return q, ErrUnsupported
		}
		if q.Text != "" {
			if q.Text[0] != '/' {
				return q, ErrQuery
			}
			for _, p := range bytes.Split([]byte(q.Text[1:]), []byte("/")) {
				if _, err = pointerToken(string(p)); err != nil {
					return q, err
				}
			}
		}
	default:
		return q, ErrUnsupported
	}
	for _, in := range q.Inputs {
		if err = ctx.Err(); err != nil {
			return q, err
		}
		if _, _, _, _, err = s.typed(ctx, in.Artifact); err != nil {
			return q, err
		}
	}
	return q, nil
}

// SaveView persists the definition and exact inputs, not unevaluated row mappings.
func (s *Service) SaveView(ctx context.Context, request QueryRequest) (id artifacts.ID, result error) {
	ctx, span := begin(ctx, "save_view", "")
	defer func() { span.End(result) }()
	q, err := s.validateView(ctx, request)
	if err != nil {
		return "", err
	}
	data, err := canon.Marshal(viewDefinition{Version: viewVersion, Query: q})
	if err != nil {
		return "", err
	}
	return s.publishFile(ctx, "view.json", bytes.NewReader(data), nil)
}
func (s *Service) GetView(ctx context.Context, id artifacts.ID) (request QueryRequest, result error) {
	ctx, span := begin(ctx, "get_view", string(id))
	defer func() { span.End(result) }()
	data, err := s.readFile(observability.WithInternal(ctx), id, "view.json", 1<<20)
	if err != nil {
		return request, err
	}
	var view viewDefinition
	if err = strictJSON(data, &view); err != nil {
		return request, err
	}
	if view.Version != viewVersion {
		return request, ErrUnsupported
	}
	return s.validateView(ctx, view.Query)
}

// Materialize consumes a query, validates the new table, and publishes compact
// NDJSON mapping evidence. OutputRevision is reserved by the caller and is not
// created here; record it before registering evidence with lineage.
func (s *Service) Materialize(ctx context.Context, request MaterializeRequest) (out Materialized, result error) {
	ctx, span := begin(ctx, "materialize", request.OutputRevision)
	defer func() {
		if result != nil {
			if !out.EvidenceRegistered {
				out.Coverage = incomplete("materialization/evidence publication did not complete")
			}
			result = &MaterializeError{Materialized: out, Err: result}
		}
		span.End(result)
	}()
	if request.OutputRevision == "" || len(request.OutputRevision) > 1024 {
		return out, fmt.Errorf("%w: exact output revision required", ErrQuery)
	}
	definition, err := s.GetSchema(observability.WithInternal(ctx), request.Schema)
	if err != nil {
		return out, err
	}
	if definition.Model != Table || request.Query.Language != SQL {
		return out, ErrUnsupported
	}
	stream, err := s.Query(ctx, request.Query)
	if err != nil {
		return out, err
	}
	defer func() { result = errors.Join(result, stream.Close()) }()
	rows, err := os.CreateTemp(s.limits.TempDir, "objects-rows-*")
	if err != nil {
		return out, err
	}
	defer cleanupTemp(rows, &result)
	details, err := os.CreateTemp(s.limits.TempDir, "objects-evidence-*")
	if err != nil {
		return out, err
	}
	defer cleanupTemp(details, &result)
	rowEncoder := json.NewEncoder(rows)
	detailEncoder := json.NewEncoder(details)
	bytesWritten := int64(0)
	for {
		row, err := stream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, err
		}
		if err = ctx.Err(); err != nil {
			return out, err
		}
		b, err := canon.Marshal(Row{ID: row.ID, Values: row.Values})
		if err != nil {
			return out, err
		}
		bytesWritten += int64(len(b)) + 1
		if bytesWritten > s.limits.MaxBytes {
			return out, ErrLimit
		}
		if err = rowEncoder.Encode(json.RawMessage(b)); err != nil {
			return out, err
		}
		if err = detailEncoder.Encode(row); err != nil {
			return out, err
		}
		out.Rows++
	}
	out.Coverage = stream.Coverage()
	if err = stream.Close(); err != nil {
		return out, err
	}
	if request.Query.RequireComplete && (out.Coverage.Rows != provenance.Complete || out.Coverage.Columns != provenance.Complete) {
		return out, ErrUnsupported
	}
	if _, err = rows.Seek(0, io.SeekStart); err != nil {
		return out, err
	}
	raw, err := s.publishFile(observability.WithInternal(ctx), "data.ndjson", rows, nil)
	if err != nil {
		return out, err
	}
	out.Artifact, err = s.Publish(ctx, PublishRequest{ValidationRequest: ValidationRequest{Data: raw, Schema: request.Schema, Encoding: NDJSON}, Metadata: request.Metadata})
	out.ArtifactRegistered = registered(out.Artifact, err)
	if err != nil {
		return out, err
	}
	if _, err = details.Seek(0, io.SeekStart); err != nil {
		return out, err
	}
	mapping, err := os.CreateTemp(s.limits.TempDir, "objects-mapping-*")
	if err != nil {
		return out, err
	}
	defer cleanupTemp(mapping, &result)
	encoder := json.NewEncoder(mapping)
	scanner := bufio.NewScanner(details)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	columnDeps := map[string][]provenance.Dependency{}
	expressions := map[string]string{}
	mappingBytes := int64(0)
	emit := func(m provenance.Mapping) error {
		if err := m.Validate(); err != nil {
			return err
		}
		encoded, err := canon.Marshal(m)
		if err != nil {
			return err
		}
		mappingBytes += int64(len(encoded)) + 1
		if mappingBytes > s.limits.MaxBytes {
			return ErrLimit
		}
		return encoder.Encode(json.RawMessage(encoded))
	}
	for scanner.Scan() {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		var row ResultRow
		if err = strictJSON(scanner.Bytes(), &row); err != nil {
			return out, err
		}
		output := provenance.Ref{Artifact: string(out.Artifact), Revision: request.OutputRevision, Row: row.ID}
		if err = emit(provenance.Mapping{Output: output, Inputs: row.Dependencies}); err != nil {
			return out, err
		}
		for _, column := range sortedKeys(row.Columns) {
			ref := output
			ref.Schema = string(request.Schema)
			ref.Column = column
			if err = emit(provenance.Mapping{Output: ref, Inputs: row.Columns[column], Expression: row.Expressions[column]}); err != nil {
				return out, err
			}
			converted := make([]provenance.Dependency, 0, len(row.Columns[column]))
			for _, dependency := range row.Columns[column] {
				if dependency.Ref.Column == "" {
					continue
				}
				dependency.Ref.Row = ""
				converted = append(converted, dependency)
			}
			columnDeps[column] = union(columnDeps[column], converted)
			expressions[column] = row.Expressions[column]
		}
	}
	if err = scanner.Err(); err != nil {
		return out, err
	}
	for _, column := range sortedKeys(columnDeps) {
		if err = emit(provenance.Mapping{Output: provenance.Ref{Artifact: string(out.Artifact), Revision: request.OutputRevision, Schema: string(request.Schema), Column: column}, Inputs: columnDeps[column], Expression: expressions[column]}); err != nil {
			return out, err
		}
	}
	if _, err = mapping.Seek(0, io.SeekStart); err != nil {
		return out, err
	}
	out.EvidencePath = "mapping.ndjson"
	out.Evidence, err = s.publishFile(observability.WithInternal(ctx), out.EvidencePath, mapping, nil)
	out.EvidenceRegistered = registered(out.Evidence, err)
	if err != nil {
		return out, err
	}
	span.Count("materialized", 1)
	return out, nil
}
func cleanupTemp(file *os.File, result *error) {
	*result = errors.Join(*result, file.Close(), os.Remove(file.Name()))
}
