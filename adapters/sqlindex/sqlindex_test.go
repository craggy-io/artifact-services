package sqlindex_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/adapters/runs"
	"github.com/craggy-io/artifact-services/adapters/sqlindex"
	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/adapters/typed"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
)

func services(s *testkit.Stack) sqlindex.Services {
	return sqlindex.Services{CAS: s.CAS, Artifacts: s.Artifacts, Objects: s.Objects, Lineage: s.Lineage, Assertions: s.Assertions}
}

func open(t *testing.T, s *testkit.Stack, path string) *sqlindex.Index {
	t.Helper()
	index, err := sqlindex.Open(sqlindex.Options{Path: path, Services: services(s)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	return index
}

// populate writes something into every indexed class of record.
func populate(t *testing.T, s *testkit.Stack) (kind assertions.KindID, input artifacts.ID) {
	t.Helper()
	ctx := s.Ctx
	input = s.File("dir/input.txt", "input")
	if _, err := s.Artifacts.RecordOccurrence(ctx, artifacts.OccurrenceRequest{ID: "import-1", Artifact: input, Name: "a.txt", Origin: "somewhere"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Artifacts.RecordOccurrence(ctx, artifacts.OccurrenceRequest{ID: "import-2", Artifact: input, Name: "b.txt"}); err != nil {
		t.Fatal(err)
	}
	kind, err := s.Assertions.Register(ctx, assertions.Kind{Name: "note", Version: 1, Shape: objects.Shape{Type: "object", Properties: map[string]objects.Shape{"level": {Type: "integer"}}}, Keyed: true}, assertions.Writer{})
	if err != nil {
		t.Fatal(err)
	}
	record := func(id, key, replaces string, level int) {
		t.Helper()
		_, err := s.Assertions.Record(ctx, assertions.Request{ID: assertions.ID(id), Kind: kind, Key: key, Replaces: assertions.ID(replaces), Subject: provenance.Ref{Occurrence: "import-1"}, Evidence: []provenance.Ref{{Artifact: string(input), Path: "dir/input.txt"}}, Value: json.RawMessage(fmt.Sprintf(`{"level":%d}`, level)), Actor: "alice"})
		if err != nil {
			t.Fatal(err)
		}
	}
	record("n1", "k1", "", 1)
	record("n2", "k1", "n1", 2)
	record("n3", "k2", "", 3)
	if err := s.Assertions.Retract(ctx, assertions.Retraction{Assertion: "n3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lineage.Append(ctx, lineage.AppendRevision{ID: "v1", History: "doc", Artifact: input}); err != nil {
		t.Fatal(err)
	}
	session := s.Session("alice")
	if _, err := session.Run(ctx, runs.Job{ID: "r1", Code: runs.Code{Kind: "b", Name: "job"}, Inputs: []provenance.Ref{{Artifact: string(input)}}}, func(ctx context.Context, r *runs.Run) error {
		return r.Blob(ctx, "out", strings.NewReader("output"), 0o644)
	}); err != nil {
		t.Fatal(err)
	}
	if err := session.End(ctx, nil); err != nil {
		t.Fatal(err)
	}
	return kind, input
}

func one[T any](t *testing.T, index *sqlindex.Index, query string, args ...any) T {
	t.Helper()
	var value T
	result, err := index.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if len(result.Rows) != 1 || len(result.Columns) != 1 {
		t.Fatalf("%s: %+v", query, result)
	}
	if err := convert(result.Rows[0][0], &value); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return value
}

func convert(from, to any) error {
	encoded, err := json.Marshal(from)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, to)
}

func TestSyncIndexesEveryLayerAndJoins(t *testing.T) {
	s := testkit.Open(t)
	kind, input := populate(t, s)
	index := open(t, s, filepath.Join(t.TempDir(), "index.db"))
	if err := index.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	if got := one[int](t, index, `SELECT count(*) FROM occurrences WHERE content = (SELECT content FROM occurrences WHERE id = 'import-1')`); got != 2 {
		t.Errorf("occurrences of equal content = %d", got)
	}
	if got := one[string](t, index, `SELECT id FROM assertions WHERE kind = ? AND key = 'k1' AND current = 1`, string(kind)); got != "n2" {
		t.Errorf("current by key = %s", got)
	}
	if got := one[int](t, index, `SELECT retracted FROM assertions WHERE id = 'n3'`); got != 1 {
		t.Errorf("retracted = %d", got)
	}
	if got := one[string](t, index, `SELECT ended_by FROM assertions WHERE id = 'n1'`); got != "n2" {
		t.Errorf("ended by = %s", got)
	}
	if got := one[int](t, index, `SELECT json_extract(value, '$.level') FROM assertions WHERE id = 'n2'`); got != 2 {
		t.Errorf("json_extract = %d", got)
	}
	// The join catalog queries and content queries cannot express alone.
	if got := one[string](t, index, `SELECT o.name FROM assertions a
		JOIN refs r ON r.owner = 'assertion' AND r.owner_id = a.id AND r.role = 'subject'
		JOIN occurrences o ON o.id = r.occurrence
		WHERE a.id = 'n2'`); got != "a.txt" {
		t.Errorf("assertion to occurrence join = %s", got)
	}
	if got := one[int](t, index, `SELECT count(*) FROM tree_entries WHERE artifact = ? AND path LIKE '%input.txt'`, string(input)); got != 1 {
		t.Errorf("path suffix count = %d", got)
	}
	if got := one[string](t, index, `SELECT o.name FROM relationships d
		JOIN refs f ON f.owner = 'relationship' AND f.owner_id = d.id AND f.role = 'from'
		JOIN refs t ON t.owner = 'relationship' AND t.owner_id = d.id AND t.role = 'to'
		JOIN occurrences o ON o.id = f.occurrence
		WHERE d.kind = ? AND t.artifact = ?`, runs.KindDerivedFrom, string(input)); got != "out" {
		t.Errorf("derivation join = %s", got)
	}
	if got := one[string](t, index, `SELECT id FROM revisions WHERE history = 'doc'`); got != "v1" {
		t.Errorf("revision = %s", got)
	}
	// A recursive lineage walk, as ad-hoc tooling writes them.
	if got := one[int](t, index, `WITH RECURSIVE walk(id) AS (
			SELECT 'v1' UNION ALL SELECT r.id FROM revisions r, json_each(r.parents) p JOIN walk w ON p.value = w.id
		) SELECT count(*) FROM walk`); got != 1 {
		t.Errorf("recursive walk = %d", got)
	}
}

func dump(t *testing.T, index *sqlindex.Index) map[string][][]any {
	t.Helper()
	all := map[string][][]any{}
	for _, table := range []string{"positions", "artifacts", "occurrences", "tree_entries", "kinds", "assertions", "activities", "relationships", "revisions", "refs", "table_rows"} {
		result, err := index.Query(context.Background(), "SELECT * FROM "+table)
		if err != nil {
			t.Fatal(err)
		}
		rows := result.Rows
		sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]) < fmt.Sprint(rows[j]) })
		all[table] = rows
	}
	return all
}

// TestDeleteAndRebuildIsIdentical is qualification scenario 9.
func TestDeleteAndRebuildIsIdentical(t *testing.T) {
	s := testkit.Open(t)
	populate(t, s)
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	index, err := sqlindex.Open(sqlindex.Options{Path: path, Services: services(s), MaxRows: 100000})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	before := dump(t, index)
	if len(before["assertions"]) != 3 || len(before["refs"]) == 0 {
		t.Fatalf("index is empty: %+v", before)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(path + "*")
	if err != nil || len(matches) == 0 {
		t.Fatalf("matches = %v err = %v", matches, err)
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil {
			t.Fatal(err)
		}
	}
	rebuilt, err := sqlindex.Open(sqlindex.Options{Path: path, Services: services(s), MaxRows: 100000})
	if err != nil {
		t.Fatal(err)
	}
	defer rebuilt.Close()
	if err := rebuilt.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	if after := dump(t, rebuilt); !reflect.DeepEqual(before, after) {
		t.Fatalf("rebuild differs:\nbefore %v\nafter  %v", before, after)
	}
	// Rebuild in place, after reopening the journals too, gives the same answer.
	if err := rebuilt.Rebuild(s.Ctx); err != nil {
		t.Fatal(err)
	}
	if after := dump(t, rebuilt); !reflect.DeepEqual(before, after) {
		t.Fatalf("in-place rebuild differs")
	}
}

func TestIncrementalSyncResumesAndMatchesAFullBuild(t *testing.T) {
	s := testkit.Open(t)
	kind, input := populate(t, s)
	dir := t.TempDir()
	incremental, err := sqlindex.Open(sqlindex.Options{Path: filepath.Join(dir, "a.db"), Services: services(s), MaxRows: 100000})
	if err != nil {
		t.Fatal(err)
	}
	defer incremental.Close()
	if err := incremental.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	positions, err := incremental.Positions(s.Ctx)
	if err != nil || positions["assertions"] == 0 || positions["lineage"] == 0 || positions["artifacts"] == 0 {
		t.Fatalf("positions = %+v err = %v", positions, err)
	}
	for i := range 1500 { // More than one page of changes.
		if _, err := s.Assertions.Record(s.Ctx, assertions.Request{ID: assertions.ID(fmt.Sprintf("more%d", i)), Kind: kind, Key: fmt.Sprintf("more-%d", i), Subject: provenance.Ref{Artifact: string(input)}, Value: json.RawMessage(`{"level":9}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Assertions.Retract(s.Ctx, assertions.Retraction{Assertion: "n2"}); err != nil {
		t.Fatal(err)
	}
	if err := incremental.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	full, err := sqlindex.Open(sqlindex.Options{Path: filepath.Join(dir, "b.db"), Services: services(s), MaxRows: 100000})
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	if err := full.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	if a, b := dump(t, incremental), dump(t, full); !reflect.DeepEqual(a, b) {
		t.Fatal("incremental sync differs from a full build")
	}
	if got := one[int](t, incremental, `SELECT current FROM assertions WHERE id = 'n2'`); got != 0 {
		t.Fatalf("retraction after sync: current = %d", got)
	}
}

func TestQueriesAreBoundedAndReadOnly(t *testing.T) {
	s := testkit.Open(t)
	populate(t, s)
	path := filepath.Join(t.TempDir(), "index.db")
	index, err := sqlindex.Open(sqlindex.Options{Path: path, Services: services(s), MaxRows: 3, Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	if err := index.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "other.db")
	for name, query := range map[string]string{
		"insert":             `INSERT INTO kinds VALUES('x','x',1,0,NULL,'{}',1)`,
		"update":             `UPDATE assertions SET current = 1`,
		"delete":             `DELETE FROM assertions`,
		"drop":               `DROP TABLE assertions`,
		"pragma":             `PRAGMA writable_schema = 1`,
		"attach":             fmt.Sprintf(`ATTACH DATABASE '%s' AS other`, other),
		"vacuum":             fmt.Sprintf(`VACUUM INTO '%s'`, other),
		"two statements":     `SELECT 1; DELETE FROM assertions`,
		"hidden second":      `SELECT 1 /* x */ ; /* y */ DELETE FROM assertions`,
		"with then delete":   `WITH x AS (SELECT 1) DELETE FROM assertions`,
		"select then attach": fmt.Sprintf(`SELECT 1; ATTACH '%s' AS o`, other),
	} {
		if _, err := index.Query(s.Ctx, query); err == nil {
			t.Errorf("%s ran", name)
		}
	}
	if got := one[int](t, index, `SELECT count(*) FROM assertions`); got != 3 {
		t.Fatalf("a rejected statement changed the index: %d", got)
	}
	if _, err := os.Stat(other); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a query touched another file: %v", err)
	}
	// A semicolon in a string is data, not a second statement.
	if got := one[string](t, index, `SELECT 'a;b'`); got != "a;b" {
		t.Errorf("string = %s", got)
	}
	if _, err := index.Query(s.Ctx, `WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 100) SELECT n FROM c`); !errors.Is(err, sqlindex.ErrRowLimit) {
		t.Errorf("over the row limit = %v, must not truncate silently", err)
	}
	if _, err := index.Query(s.Ctx, `WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c) SELECT count(*) FROM c`); err == nil {
		t.Error("runaway query outlived the time limit")
	}
}

type noteRow struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Current int    `json:"current"`
}

func TestScanRowsDecodesIntoStructs(t *testing.T) {
	s := testkit.Open(t)
	populate(t, s)
	index := open(t, s, filepath.Join(t.TempDir(), "index.db"))
	if err := index.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	var got []noteRow
	for row, err := range sqlindex.ScanRows[noteRow](s.Ctx, index, `SELECT id, kind, current FROM assertions WHERE current = ? ORDER BY id`, 1) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if len(got) != 1 || got[0].ID != "n2" || got[0].Kind != "note@1" || got[0].Current != 1 {
		t.Fatalf("rows = %+v", got)
	}
	for range sqlindex.ScanRows[noteRow](s.Ctx, index, `SELECT id, kind, current FROM assertions`) {
		break // Early exit releases the query.
	}
	for _, err := range sqlindex.ScanRows[noteRow](s.Ctx, index, `DELETE FROM assertions`) {
		if !errors.Is(err, sqlindex.ErrReadOnly) {
			t.Fatalf("write through ScanRows = %v", err)
		}
	}
}

func TestMaterializeTableOnDemand(t *testing.T) {
	type row struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	s := testkit.Open(t)
	table, err := typed.DefineTable[row](s.Ctx, typed.Services{CAS: s.CAS, Artifacts: s.Artifacts, Objects: s.Objects}, "name")
	if err != nil {
		t.Fatal(err)
	}
	id, err := table.Put(s.Ctx, typed.Slice([]row{{"a", 1}, {"b", 2}}), nil)
	if err != nil {
		t.Fatal(err)
	}
	index := open(t, s, filepath.Join(t.TempDir(), "index.db"))
	if err := index.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	if got := one[int](t, index, `SELECT count(*) FROM table_rows`); got != 0 {
		t.Fatalf("rows copied without being asked: %d", got)
	}
	if n, err := index.MaterializeTable(s.Ctx, id); err != nil || n != 2 {
		t.Fatalf("materialized %d err = %v", n, err)
	}
	if _, err := index.MaterializeTable(s.Ctx, id); err != nil {
		t.Fatalf("again = %v", err)
	}
	if got := one[int](t, index, `SELECT sum(json_extract(row_values, '$.count')) FROM table_rows WHERE artifact = ?`, string(id)); got != 3 {
		t.Fatalf("sum = %d", got)
	}
}

func TestSnapshotIsAStandaloneDatabaseArtifact(t *testing.T) {
	s := testkit.Open(t)
	populate(t, s)
	index := open(t, s, filepath.Join(t.TempDir(), "index.db"))
	snapshot, err := index.PublishSnapshot(s.Ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Positions["assertions"] == 0 {
		t.Fatalf("positions = %+v", snapshot.Positions)
	}
	reader, err := s.Artifacts.OpenFile(s.Ctx, snapshot.Artifact, sqlindex.SnapshotFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	copyPath := filepath.Join(t.TempDir(), "copy.db")
	file, err := os.Create(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadFrom(reader); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(copyPath)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM assertions`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("count = %d err = %v", count, err)
	}
}

func TestFollowKeepsUpAndStops(t *testing.T) {
	s := testkit.Open(t)
	kind, input := populate(t, s)
	index := open(t, s, filepath.Join(t.TempDir(), "index.db"))
	if err := index.Sync(s.Ctx); err != nil {
		t.Fatal(err)
	}
	stop := index.Follow(s.Ctx, func(err error) { t.Error(err) })
	defer stop()
	if _, err := s.Assertions.Record(s.Ctx, assertions.Request{ID: "followed", Kind: kind, Key: "f", Subject: provenance.Ref{Artifact: string(input)}, Value: json.RawMessage(`{"level":1}`)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		result, err := index.Query(s.Ctx, `SELECT id FROM assertions WHERE id = 'followed'`)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never caught up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSchemaVersionMismatchRefusesToOpen(t *testing.T) {
	s := testkit.Open(t)
	path := filepath.Join(t.TempDir(), "index.db")
	index, err := sqlindex.Open(sqlindex.Options{Path: path, Services: services(s)})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, sqlindex.SchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlindex.Open(sqlindex.Options{Path: path, Services: services(s)}); !errors.Is(err, sqlindex.ErrSchemaVersion) {
		t.Fatalf("mismatch = %v", err)
	}
}
