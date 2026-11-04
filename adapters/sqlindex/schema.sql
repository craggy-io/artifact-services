-- The queryable surface of sqlindex. Names here are an API: changing one means
-- changing SchemaVersion. "internal" is 1 for records written by supporting work,
-- which user-facing queries usually want to exclude.

CREATE TABLE positions (
	journal  TEXT PRIMARY KEY,
	sequence INTEGER NOT NULL
);

CREATE TABLE artifacts (
	id            TEXT PRIMARY KEY,
	file_count    INTEGER NOT NULL,
	logical_bytes INTEGER NOT NULL,
	schema_id     TEXT,
	content       TEXT NOT NULL,
	sequence      INTEGER NOT NULL,
	internal      INTEGER NOT NULL
);
CREATE INDEX artifacts_content ON artifacts(content);
CREATE INDEX artifacts_schema ON artifacts(schema_id);

CREATE TABLE occurrences (
	id       TEXT PRIMARY KEY,
	artifact TEXT NOT NULL,
	content  TEXT NOT NULL,
	name     TEXT,
	origin   TEXT,
	producer TEXT,
	metadata TEXT,
	sequence INTEGER NOT NULL,
	internal INTEGER NOT NULL
);
CREATE INDEX occurrences_artifact ON occurrences(artifact);
CREATE INDEX occurrences_content ON occurrences(content);
CREATE INDEX occurrences_name ON occurrences(name);

-- Links appear as stored, not expanded. kind is file, dir or link.
CREATE TABLE tree_entries (
	artifact    TEXT NOT NULL,
	path        TEXT NOT NULL,
	kind        TEXT NOT NULL,
	content     TEXT,
	size        INTEGER NOT NULL,
	mode        INTEGER NOT NULL,
	target      TEXT,
	target_path TEXT,
	PRIMARY KEY (artifact, path)
);
CREATE INDEX tree_entries_content ON tree_entries(content);

CREATE TABLE kinds (
	id       TEXT PRIMARY KEY,
	name     TEXT NOT NULL,
	version  INTEGER NOT NULL,
	keyed    INTEGER NOT NULL,
	writers  TEXT,
	shape    TEXT NOT NULL,
	sequence INTEGER NOT NULL
);

-- current is derived by replaying commits, so it is true as of positions.
CREATE TABLE assertions (
	id        TEXT PRIMARY KEY,
	kind      TEXT NOT NULL,
	key       TEXT,
	value     TEXT NOT NULL,
	replaces  TEXT,
	activity  TEXT,
	actor     TEXT,
	writer    TEXT,
	sequence  INTEGER NOT NULL,
	current   INTEGER NOT NULL,
	retracted INTEGER NOT NULL,
	ended_at  INTEGER,
	ended_by  TEXT,
	internal  INTEGER NOT NULL
);
CREATE INDEX assertions_kind ON assertions(kind, current);
CREATE INDEX assertions_key ON assertions(kind, key);

CREATE TABLE activities (
	id              TEXT PRIMARY KEY,
	kind            TEXT NOT NULL,
	actor           TEXT,
	external_source TEXT,
	metadata        TEXT,
	sequence        INTEGER NOT NULL,
	internal        INTEGER NOT NULL
);
CREATE INDEX activities_kind ON activities(kind);

CREATE TABLE relationships (
	id       TEXT PRIMARY KEY,
	kind     TEXT NOT NULL,
	activity TEXT,
	metadata TEXT,
	sequence INTEGER NOT NULL,
	internal INTEGER NOT NULL
);
CREATE INDEX relationships_kind ON relationships(kind);

CREATE TABLE revisions (
	id       TEXT PRIMARY KEY,
	artifact TEXT NOT NULL,
	history  TEXT,
	parents  TEXT,
	activity TEXT,
	metadata TEXT,
	sequence INTEGER NOT NULL,
	internal INTEGER NOT NULL
);
CREATE INDEX revisions_history ON revisions(history);
CREATE INDEX revisions_artifact ON revisions(artifact);

-- Every reference held by any record, flattened. owner is activity, relationship or
-- assertion. role is input, output, from, to, subject or evidence.
CREATE TABLE refs (
	owner        TEXT NOT NULL,
	owner_id     TEXT NOT NULL,
	role         TEXT NOT NULL,
	position     INTEGER NOT NULL,
	form         TEXT NOT NULL,
	artifact     TEXT,
	revision     TEXT,
	schema_id    TEXT,
	row_id       TEXT,
	column_id    TEXT,
	occurrence   TEXT,
	digest       TEXT,
	path         TEXT,
	range_offset INTEGER NOT NULL,
	range_length INTEGER NOT NULL,
	assertion    TEXT,
	activity     TEXT,
	url          TEXT,
	retrieval    TEXT,
	PRIMARY KEY (owner, owner_id, role, position)
);
CREATE INDEX refs_artifact ON refs(artifact);
CREATE INDEX refs_occurrence ON refs(occurrence);
CREATE INDEX refs_digest ON refs(digest);
CREATE INDEX refs_assertion ON refs(assertion);
CREATE INDEX refs_revision ON refs(revision);

-- Filled only by MaterializeTable. row_values is a JSON object keyed by column ID.
CREATE TABLE table_rows (
	artifact   TEXT NOT NULL,
	row_id     TEXT NOT NULL,
	position   INTEGER NOT NULL,
	row_values TEXT NOT NULL,
	PRIMARY KEY (artifact, row_id)
);
