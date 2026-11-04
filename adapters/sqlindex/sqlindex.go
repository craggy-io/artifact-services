// Package sqlindex keeps a SQLite read model of a service stack, for ad-hoc queries
// that join across layers. It is always derived: deleting the database and syncing
// again produces the same answers. It lives in its own Go module so that the core
// module stays free of dependencies.
//
// The schema is versioned and is an API, despite being derived: callers write SQL
// against it, so its table and column names are a compatibility surface exactly like
// exported Go names. SchemaVersion changes when they do. An index at another version
// refuses to open until rebuilt.
//
// Consistency is "as of Positions": each page of changes applies in one transaction,
// so a reader sees every journal at some committed position, but positions of
// different journals are unrelated, and a record referring across journals can be
// indexed before its target. That is the same absence of a cross-layer transaction the
// journals have.
package sqlindex

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
	_ "modernc.org/sqlite"
)

// SchemaVersion identifies the table and column names documented in schema.sql.
const SchemaVersion = 1

//go:embed schema.sql
var schemaSQL string

const (
	journalArtifacts  = "artifacts"
	journalLineage    = "lineage"
	journalAssertions = "assertions"
	pageSize          = 1000
)

var (
	// ErrSchemaVersion reports an index built by another schema version. Rebuild it.
	ErrSchemaVersion = errors.New("sqlindex: index has another schema version")
	// ErrReadOnly reports a statement other than one read-only query.
	ErrReadOnly = errors.New("sqlindex: only a single SELECT is accepted")
	// ErrRowLimit reports a result over the row limit. Results are never truncated
	// silently: narrow the query or raise the limit.
	ErrRowLimit = errors.New("sqlindex: row limit exceeded")
)

// Services are the layers indexed. Objects is needed only for MaterializeTable.
type Services struct {
	CAS        cas.Store
	Artifacts  *artifacts.Service
	Objects    *objects.Service
	Lineage    *lineage.Service
	Assertions *assertions.Service
}

// Options configures an index. Path is the database file. MaxRows and Timeout bound
// each Query, defaulting to 10,000 rows and 30 seconds.
type Options struct {
	Path     string
	Services Services
	MaxRows  int
	Timeout  time.Duration
}

// Index is a derived SQLite database. Sync and Rebuild serialize with each other, and
// queries run on a separate read-only connection, so they may run at any time.
type Index struct {
	options Options
	write   *sql.DB
	read    *sql.DB
	mu      sync.Mutex
}

// Open opens or creates the index without syncing it.
func Open(options Options) (*Index, error) {
	if options.Path == "" || options.Services.Artifacts == nil {
		return nil, errors.New("sqlindex: Path and Services.Artifacts are required")
	}
	if options.MaxRows == 0 {
		options.MaxRows = 10000
	}
	if options.Timeout == 0 {
		options.Timeout = 30 * time.Second
	}
	if err := os.MkdirAll(filepath.Dir(options.Path), 0o755); err != nil {
		return nil, err
	}
	write, err := sql.Open("sqlite", dsn(options.Path, "rwc", "journal_mode(WAL)", "busy_timeout(10000)"))
	if err != nil {
		return nil, err
	}
	write.SetMaxOpenConns(1)
	index := &Index{options: options, write: write}
	if err := index.migrate(); err != nil {
		return nil, errors.Join(err, write.Close())
	}
	index.read, err = sql.Open("sqlite", dsn(options.Path, "ro", "query_only(1)", "busy_timeout(10000)"))
	if err != nil {
		return nil, errors.Join(err, write.Close())
	}
	return index, nil
}

func dsn(path, mode string, pragmas ...string) string {
	query := url.Values{"mode": {mode}}
	for _, pragma := range pragmas {
		query.Add("_pragma", pragma)
	}
	return (&url.URL{Scheme: "file", Opaque: filepath.ToSlash(path), RawQuery: query.Encode()}).String()
}

func (x *Index) migrate() error {
	var version int
	if err := x.write.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	switch version {
	case SchemaVersion:
		return nil
	case 0:
		if _, err := x.write.Exec(schemaSQL); err != nil {
			return err
		}
		_, err := x.write.Exec(fmt.Sprintf("PRAGMA user_version = %d", SchemaVersion))
		return err
	}
	return fmt.Errorf("%w: found %d, want %d", ErrSchemaVersion, version, SchemaVersion)
}

// Close closes both connections.
func (x *Index) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return errors.Join(x.read.Close(), x.write.Close())
}

// Positions returns each journal's last indexed sequence.
func (x *Index) Positions(ctx context.Context) (map[string]uint64, error) {
	rows, err := x.read.QueryContext(ctx, "SELECT journal, sequence FROM positions")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	positions := map[string]uint64{}
	for rows.Next() {
		var journal string
		var sequence uint64
		if err := rows.Scan(&journal, &sequence); err != nil {
			return nil, err
		}
		positions[journal] = sequence
	}
	return positions, rows.Err()
}

type source struct {
	journal string
	read    func(context.Context, provenance.ChangeQuery) (provenance.ChangePage, error)
	apply   func(context.Context, *sql.Tx, provenance.Change) error
}

func (x *Index) sources() []source {
	s := x.options.Services
	sources := []source{{journalArtifacts, s.Artifacts.Changes, x.applyArtifact}}
	if s.Lineage != nil {
		sources = append(sources, source{journalLineage, s.Lineage.Changes, x.applyLineage})
	}
	if s.Assertions != nil {
		sources = append(sources, source{journalAssertions, s.Assertions.Changes, x.applyAssertion})
	}
	return sources
}

// Sync applies every change since the stored positions, resuming losslessly after a
// crash or a dropped notification. A crash leaves the last whole page applied.
func (x *Index) Sync(ctx context.Context) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, s := range x.sources() {
		if err := x.syncJournal(ctx, s); err != nil {
			return fmt.Errorf("sqlindex: sync %s: %w", s.journal, err)
		}
	}
	return nil
}

func (x *Index) syncJournal(ctx context.Context, s source) error {
	var position uint64
	err := x.write.QueryRowContext(ctx, "SELECT sequence FROM positions WHERE journal = ?", s.journal).Scan(&position)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	for {
		page, err := s.read(ctx, provenance.ChangeQuery{After: position, Limit: pageSize, IncludeInternal: true})
		if err != nil {
			return err
		}
		if page.Last == position {
			return nil
		}
		if err := x.applyPage(ctx, s, page); err != nil {
			return err
		}
		position = page.Last
		if len(page.Changes) < pageSize {
			return nil
		}
	}
}

func (x *Index) applyPage(ctx context.Context, s source, page provenance.ChangePage) (result error) {
	tx, err := x.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, tx.Rollback())
		}
	}()
	for _, change := range page.Changes {
		if err := s.apply(ctx, tx, change); err != nil {
			return fmt.Errorf("%s %s: %w", change.Class, change.ID, err)
		}
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO positions(journal, sequence) VALUES(?, ?) ON CONFLICT(journal) DO UPDATE SET sequence = excluded.sequence", s.journal, page.Last)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Follow syncs whenever a journal commits, until stop is called. Notifications
// coalesce and Sync reads from stored positions, so a burst is one catch-up. Errors go
// to report, which may be nil, and the next commit tries again.
func (x *Index) Follow(ctx context.Context, report func(error)) (stop func()) {
	wake := make(chan struct{}, 1)
	notify := func(uint64) {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	s := x.options.Services
	stops := []func(){s.Artifacts.OnCommit(notify)}
	if s.Lineage != nil {
		stops = append(stops, s.Lineage.OnCommit(notify))
	}
	if s.Assertions != nil {
		stops = append(stops, s.Assertions.OnCommit(notify))
	}
	follow, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-follow.Done():
				return
			case <-wake:
			}
			if err := x.Sync(follow); err != nil && report != nil && follow.Err() == nil {
				report(err)
			}
		}
	}()
	return func() {
		for _, s := range stops {
			s()
		}
		cancel()
		<-done
	}
}

// Rebuild discards everything and syncs from sequence zero, the recovery path for any
// doubt about the index. Tables materialized on demand must be requested again.
func (x *Index) Rebuild(ctx context.Context) error {
	x.mu.Lock()
	for _, table := range indexedTables {
		if _, err := x.write.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			x.mu.Unlock()
			return err
		}
	}
	x.mu.Unlock()
	return x.Sync(ctx)
}

var indexedTables = []string{"positions", "artifacts", "occurrences", "tree_entries", "kinds", "assertions", "activities", "relationships", "revisions", "refs", "table_rows"}

func encodeJSON(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(v) == 0 {
			return nil, nil
		}
		return string(v), nil
	case map[string]json.RawMessage:
		if len(v) == 0 {
			return nil, nil
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func boolean(b bool) int {
	if b {
		return 1
	}
	return 0
}

// nullable keeps absent strings NULL, so that SQL can distinguish them.
func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// insertRef flattens a reference, so that queries join on columns, not JSON.
func insertRef(ctx context.Context, tx *sql.Tx, owner, ownerID, role string, position int, ref provenance.Ref) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO refs(owner, owner_id, role, position, form, artifact, revision, schema_id, row_id, column_id, occurrence, digest, path, range_offset, range_length, assertion, activity, url, retrieval)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		owner, ownerID, role, position, ref.Form(), nullable(ref.Artifact), nullable(ref.Revision), nullable(ref.Schema), nullable(ref.Row), nullable(ref.Column), nullable(ref.Occurrence), nullable(ref.Digest), nullable(ref.Path), ref.Offset, ref.Length, nullable(ref.Assertion), nullable(ref.Activity), nullable(ref.URL), nullable(ref.Retrieval))
	return err
}

func insertRefs(ctx context.Context, tx *sql.Tx, owner, ownerID, role string, refs []provenance.Ref) error {
	for i, ref := range refs {
		if err := insertRef(ctx, tx, owner, ownerID, role, i, ref); err != nil {
			return err
		}
	}
	return nil
}

func (x *Index) applyArtifact(ctx context.Context, tx *sql.Tx, change provenance.Change) error {
	store := x.options.Services.Artifacts
	if change.Class == artifacts.ChangeOccurrence {
		o, err := store.GetOccurrence(ctx, artifacts.OccurrenceID(change.ID))
		if err != nil {
			return err
		}
		metadata, err := encodeJSON(o.Metadata)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO occurrences(id, artifact, content, name, origin, producer, metadata, sequence, internal) VALUES(?,?,?,?,?,?,?,?,?)`,
			string(o.ID), string(o.Artifact), string(o.Content), nullable(o.Name), nullable(o.Origin), nullable(o.Producer), metadata, o.Sequence, boolean(change.Internal))
		return err
	}
	id := artifacts.ID(change.ID)
	page, err := store.Search(ctx, artifacts.CatalogQuery{ID: id, IncludeInternal: true, Limit: 1})
	if err != nil {
		return err
	}
	if len(page.Items) != 1 {
		return fmt.Errorf("catalog has %d items for this ID", len(page.Items))
	}
	item := page.Items[0]
	_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO artifacts(id, file_count, logical_bytes, schema_id, content, sequence, internal) VALUES(?,?,?,?,?,?,?)`,
		string(item.ID), item.FileCount, item.LogicalBytes, nullable(string(item.SchemaID)), string(item.Content), change.Sequence, boolean(change.Internal))
	if err != nil {
		return err
	}
	return x.indexTree(ctx, tx, id)
}

func (x *Index) indexTree(ctx context.Context, tx *sql.Tx, id artifacts.ID) error {
	statement, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO tree_entries(artifact, path, kind, content, size, mode, target, target_path) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer statement.Close()
	cursor := ""
	for {
		page, err := x.options.Services.Artifacts.ListFiles(ctx, id, artifacts.FileQuery{Limit: 1000, Cursor: cursor})
		if err != nil {
			return err
		}
		for _, e := range page.Entries {
			kind := string(e.Kind)
			if kind == "" {
				kind = "file"
			}
			if _, err := statement.ExecContext(ctx, string(id), e.Path, kind, nullable(string(e.Content)), e.Size, e.Mode, nullable(string(e.Target)), nullable(e.TargetPath)); err != nil {
				return err
			}
		}
		if cursor = page.NextCursor; cursor == "" {
			return nil
		}
	}
}

func (x *Index) applyLineage(ctx context.Context, tx *sql.Tx, change provenance.Change) error {
	record, err := x.options.Services.Lineage.Get(ctx, lineage.ID(change.ID))
	if err != nil {
		return err
	}
	id := string(record.ID)
	internal := boolean(change.Internal)
	switch {
	case record.Activity != nil:
		a := record.Activity
		metadata, err := encodeJSON(a.Metadata)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO activities(id, kind, actor, external_source, metadata, sequence, internal) VALUES(?,?,?,?,?,?,?)`,
			id, a.Kind, nullable(a.Actor), nullable(a.ExternalSource), metadata, record.Sequence, internal); err != nil {
			return err
		}
		if err := insertRefs(ctx, tx, "activity", id, "input", a.Inputs); err != nil {
			return err
		}
		return insertRefs(ctx, tx, "activity", id, "output", a.Outputs)
	case record.Relationship != nil:
		r := record.Relationship
		metadata, err := encodeJSON(r.Metadata)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO relationships(id, kind, activity, metadata, sequence, internal) VALUES(?,?,?,?,?,?)`,
			id, r.Kind, nullable(string(r.Activity)), metadata, record.Sequence, internal); err != nil {
			return err
		}
		if err := insertRef(ctx, tx, "relationship", id, "from", 0, r.From); err != nil {
			return err
		}
		return insertRef(ctx, tx, "relationship", id, "to", 0, r.To)
	case record.Revision != nil:
		v := record.Revision
		metadata, err := encodeJSON(v.Metadata)
		if err != nil {
			return err
		}
		parents, err := encodeJSON(v.Parents)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO revisions(id, artifact, history, parents, activity, metadata, sequence, internal) VALUES(?,?,?,?,?,?,?,?)`,
			id, string(v.Artifact), nullable(v.History), parents, nullable(string(v.Activity)), metadata, record.Sequence, internal)
		return err
	}
	return errors.New("record has no payload")
}

func (x *Index) applyAssertion(ctx context.Context, tx *sql.Tx, change provenance.Change) error {
	service := x.options.Services.Assertions
	switch change.Class {
	case assertions.ChangeKind:
		kind, err := service.GetKind(ctx, assertions.KindID(change.ID))
		if err != nil {
			return err
		}
		writers, err := encodeJSON(kind.Writers)
		if err != nil {
			return err
		}
		shape, err := encodeJSON(kind.Shape)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO kinds(id, name, version, keyed, writers, shape, sequence) VALUES(?,?,?,?,?,?,?)`,
			change.ID, kind.Name, kind.Version, boolean(kind.Keyed), writers, shape, change.Sequence)
		return err
	case assertions.ChangeRetraction:
		_, err := tx.ExecContext(ctx, `UPDATE assertions SET current = 0, retracted = 1, ended_at = ? WHERE id = ?`, change.Sequence, change.ID)
		return err
	}
	// State comes from replaying events, not from the service's present answer, so a
	// reader at an old position sees that position's truth.
	a, err := service.Get(ctx, assertions.ID(change.ID))
	if err != nil {
		return err
	}
	id := string(a.ID)
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO assertions(id, kind, key, value, replaces, activity, actor, writer, sequence, current, retracted, ended_at, ended_by, internal) VALUES(?,?,?,?,?,?,?,?,?,1,0,NULL,NULL,?)`,
		id, string(a.Kind), nullable(a.Key), string(a.Value), nullable(string(a.Replaces)), nullable(a.Activity), nullable(a.Actor), nullable(a.Writer), a.Sequence, boolean(change.Internal)); err != nil {
		return err
	}
	if a.Replaces != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE assertions SET current = 0, ended_at = ?, ended_by = ? WHERE id = ?`, a.Sequence, id, string(a.Replaces)); err != nil {
			return err
		}
	}
	if err := insertRef(ctx, tx, "assertion", id, "subject", 0, a.Subject); err != nil {
		return err
	}
	return insertRefs(ctx, tx, "assertion", id, "evidence", a.Evidence)
}

// MaterializeTable copies a validated table's rows into table_rows on demand, since
// most tables are never queried this way. It is idempotent, and the copy goes with the
// database: Rebuild forgets it.
func (x *Index) MaterializeTable(ctx context.Context, id artifacts.ID) (count int64, result error) {
	if x.options.Services.Objects == nil {
		return 0, errors.New("sqlindex: Services.Objects is required to materialize a table")
	}
	stream, err := x.options.Services.Objects.Rows(ctx, id)
	if err != nil {
		return 0, err
	}
	defer func() { result = errors.Join(result, stream.Close()) }()
	x.mu.Lock()
	defer x.mu.Unlock()
	tx, err := x.write.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, tx.Rollback())
		}
	}()
	statement, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO table_rows(artifact, row_id, position, row_values) VALUES(?,?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer statement.Close()
	for {
		row, err := stream.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, err
		}
		values, err := json.Marshal(row.Values)
		if err != nil {
			return 0, err
		}
		if _, err := statement.ExecContext(ctx, string(id), row.ID, count, string(values)); err != nil {
			return 0, err
		}
		count++
	}
	return count, tx.Commit()
}
