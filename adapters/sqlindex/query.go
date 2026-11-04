package sqlindex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"os"
	"reflect"
	"strings"

	"github.com/craggy-io/artifact-services/artifacts"
)

// Result is a whole query result, within the row limit.
type Result struct {
	Columns []string
	Rows    [][]any
}

// requireSelect accepts one statement that starts with SELECT or WITH. The connection
// is opened read-only with query_only set, so writes fail regardless. This check is
// what also keeps out statements a read-only connection still permits, ATTACH being
// the one that matters: it would let SQL read any other database file on the machine.
func requireSelect(query string) error {
	body := strings.TrimSpace(query)
	first, _, _ := strings.Cut(strings.ToUpper(body), " ")
	first = strings.TrimSpace(first)
	if first != "SELECT" && first != "WITH" {
		return ErrReadOnly
	}
	// One statement: nothing but whitespace after a semicolon outside a string,
	// quoted name or comment.
	var quote byte
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '[':
			quote = ']'
		case c == '-' && i+1 < len(body) && body[i+1] == '-':
			end := strings.IndexByte(body[i:], '\n')
			if end < 0 {
				return nil
			}
			i += end
		case c == '/' && i+1 < len(body) && body[i+1] == '*':
			end := strings.Index(body[i+2:], "*/")
			if end < 0 {
				return ErrReadOnly
			}
			i += end + 3
		case c == ';':
			if strings.TrimSpace(body[i+1:]) != "" {
				return ErrReadOnly
			}
		}
	}
	return nil
}

func normalizeCell(value any) any {
	if raw, ok := value.([]byte); ok {
		return string(raw)
	}
	return value
}

// each runs one read-only query under the row and time limits, calling visit with
// reused cell storage. visit returning false stops early without error.
func (x *Index) each(ctx context.Context, query string, args []any, begin func([]string), visit func([]any) bool) error {
	if err := requireSelect(query); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, x.options.Timeout)
	defer cancel()
	rows, err := x.read.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	begin(columns)
	cells := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range cells {
		pointers[i] = &cells[i]
	}
	count := 0
	for rows.Next() {
		if count++; count > x.options.MaxRows {
			return fmt.Errorf("%w: more than %d rows", ErrRowLimit, x.options.MaxRows)
		}
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
		for i := range cells {
			cells[i] = normalizeCell(cells[i])
		}
		if !visit(cells) {
			return nil
		}
	}
	return rows.Err()
}

// Rows streams a query's rows under the row and time limits, as column-keyed maps.
func (x *Index) Rows(ctx context.Context, query string, args ...any) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		var names []string
		stopped := false
		err := x.each(ctx, query, args, func(columns []string) { names = columns }, func(cells []any) bool {
			row := make(map[string]any, len(names))
			for i, name := range names {
				row[name] = cells[i]
			}
			stopped = !yield(row, nil)
			return !stopped
		})
		if err != nil && !stopped {
			yield(nil, err)
		}
	}
}

// Query runs one read-only SELECT and returns all of its rows, or ErrRowLimit.
func (x *Index) Query(ctx context.Context, query string, args ...any) (Result, error) {
	result := Result{}
	err := x.each(ctx, query, args, func(columns []string) { result.Columns = columns }, func(cells []any) bool {
		result.Rows = append(result.Rows, append([]any(nil), cells...))
		return true
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

// ScanRows decodes each row into T through JSON, so column names match JSON tags and a
// JSON text column stays a string. It adds no limits beyond those of Rows.
func ScanRows[T any](ctx context.Context, x *Index, query string, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		for row, err := range x.Rows(ctx, query, args...) {
			if err != nil {
				yield(zero, err)
				return
			}
			var value T
			if kind := reflect.TypeFor[T]().Kind(); kind != reflect.Struct && kind != reflect.Map {
				err = errors.New("sqlindex: ScanRows needs a struct or map type")
			} else {
				var encoded []byte
				if encoded, err = json.Marshal(row); err == nil {
					err = json.Unmarshal(encoded, &value)
				}
			}
			if !yield(value, err) || err != nil {
				return
			}
		}
	}
}

// Snapshot is a point-in-time copy of the index published as an artifact.
type Snapshot struct {
	Artifact  artifacts.ID
	Positions map[string]uint64
}

// SnapshotFile is the path of the database inside a snapshot artifact.
const SnapshotFile = "index.sqlite"

// Export writes a consistent copy of the index to a new standalone database file.
func (x *Index) Export(ctx context.Context, path string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	_, err := x.write.ExecContext(ctx, "VACUUM INTO ?", path)
	return err
}

// PublishSnapshot exports the index and publishes the copy as a file artifact with its
// positions in the metadata. Being derived, it adds no authority: a reader should treat
// it as a report of those positions.
func (x *Index) PublishSnapshot(ctx context.Context, dir string) (snapshot Snapshot, result error) {
	temp, err := os.MkdirTemp(dir, "sqlindex-snapshot-*")
	if err != nil {
		return snapshot, err
	}
	defer func() { result = errors.Join(result, os.RemoveAll(temp)) }()
	path := temp + string(os.PathSeparator) + SnapshotFile
	if err := x.Sync(ctx); err != nil {
		return snapshot, err
	}
	if err := x.Export(ctx, path); err != nil {
		return snapshot, err
	}
	if snapshot.Positions, err = x.Positions(ctx); err != nil {
		return snapshot, err
	}
	file, err := os.Open(path)
	if err != nil {
		return snapshot, err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	put, err := x.options.Services.CAS.Put(ctx, file)
	if err != nil {
		return snapshot, err
	}
	positions, err := json.Marshal(snapshot.Positions)
	if err != nil {
		return snapshot, err
	}
	snapshot.Artifact, err = x.options.Services.Artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: SnapshotFile, Content: put.ID, Size: put.Size}}, Metadata: map[string]json.RawMessage{"sqlindex.positions": positions, "sqlindex.schema": json.RawMessage(fmt.Sprint(SchemaVersion))}})
	return snapshot, err
}
