package stack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/craggy-io/artifact-services/adapters/typed"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/objects"
)

// TableSchema is a table schema whose key columns are non-nullable and every
// other column nullable, whatever the columns say.
func TableSchema(key []string, cols ...objects.Column) objects.SchemaDefinition {
	isKey := map[string]bool{}
	for _, k := range key {
		isKey[k] = true
	}
	def := objects.SchemaDefinition{Version: objects.SchemaVersion, Model: objects.Table, Key: key}
	for _, c := range cols {
		c.Shape.Nullable = !isKey[c.ID]
		def.Columns = append(def.Columns, c)
	}
	return def
}

// PublishTable publishes rows as a table artifact validated against def. The
// schema is defined once per assertions service and memo key (typed.Memo);
// publishing under the same key with a different definition fails, so a
// changed column set needs a new key. Each row is one record's values, by
// column ID.
func (s *Stack) PublishTable(ctx context.Context, memo string, def objects.SchemaDefinition, rows []map[string]any) (artifacts.ID, error) {
	fp, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	type schema struct {
		id artifacts.ID
		fp string
	}
	sc, err := typed.Memo(ctx, s.Assertions, memo, func(ctx context.Context) (schema, error) {
		id, err := s.Objects.DefineSchema(ctx, def)
		return schema{id, string(fp)}, err
	})
	if err != nil {
		return "", fmt.Errorf("define table schema: %w", err)
	}
	if sc.fp != string(fp) {
		return "", fmt.Errorf("table %s already defined with different columns", memo)
	}
	var buf bytes.Buffer
	for _, row := range rows {
		line, err := json.Marshal(map[string]any{"values": row})
		if err != nil {
			return "", err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	put, err := s.CAS.Put(ctx, &buf)
	if err != nil {
		return "", err
	}
	raw, err := s.Artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "data.ndjson", Content: put.ID, Size: put.Size}}})
	if err != nil {
		return "", err
	}
	id, err := s.Objects.Publish(ctx, objects.PublishRequest{ValidationRequest: objects.ValidationRequest{Data: raw, Schema: sc.id, Encoding: objects.NDJSON}})
	if err != nil {
		return "", fmt.Errorf("publish table: %w", err)
	}
	return id, nil
}
