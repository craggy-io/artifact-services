// Package provenance defines neutral, version-qualified references and evidence.
package provenance

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// GrammarVersion is the reference grammar this module understands. A Ref naming
// any other version is rejected instead of being stored without being understood.
const GrammarVersion = 1

// Reference forms. A Ref has exactly one form, and Validate rejects anything else.
const (
	FormSnapshot   = "snapshot"   // Artifact and/or Revision: a whole immutable snapshot.
	FormRow        = "row"        // Artifact + Revision + Row: a row by stable identity.
	FormCell       = "cell"       // Artifact + Revision + Schema + Row + Column.
	FormColumn     = "column"     // Artifact + Revision + Schema + Column.
	FormOccurrence = "occurrence" // Occurrence: one publication, not its content.
	FormContent    = "content"    // Digest, optional range: bytes wherever they occur.
	FormPath       = "path"       // Artifact or Occurrence + Path, optional range.
	FormAssertion  = "assertion"  // Assertion: another assertion record.
	FormActivity   = "activity"   // Activity: an activity or run record.
	FormExternal   = "external"   // URL + Retrieval: an external source snapshot.
)

// ErrUnsupportedForm reports a well-formed reference whose target this layer was
// not configured to check. It is never stored unchecked.
var ErrUnsupportedForm = errors.New("provenance: reference form cannot be checked here")

// Ref keeps identity separate from display names and offsets. It stays comparable
// because references key traversal adjacency. A range is Length > 0 at Offset.
//
// A Digest reference follows content: it means these bytes wherever they occur.
// An Occurrence reference means one publication and never follows content. An
// Artifact reference means one immutable snapshot, shared by every occurrence of it.
type Ref struct {
	Version    int    `json:"version,omitempty"`
	Artifact   string `json:"artifact,omitempty"`
	Revision   string `json:"revision,omitempty"`
	Schema     string `json:"schema,omitempty"`
	Row        string `json:"row,omitempty"`
	Column     string `json:"column,omitempty"`
	Occurrence string `json:"occurrence,omitempty"`
	Digest     string `json:"digest,omitempty"`
	Path       string `json:"path,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	Length     int64  `json:"length,omitempty"`
	Assertion  string `json:"assertion,omitempty"`
	Activity   string `json:"activity,omitempty"`
	URL        string `json:"url,omitempty"`
	Retrieval  string `json:"retrieval,omitempty"`
}

// Detailed reports a table selector, which only a schema-aware validator can check.
func (r Ref) Detailed() bool { return r.Row != "" || r.Column != "" }

func (r Ref) hasRange() bool { return r.Offset != 0 || r.Length != 0 }

// Form classifies a reference. It returns "" for a combination in no valid form.
func (r Ref) Form() string {
	content := r.Digest != ""
	located := r.Occurrence != "" || r.Path != "" || r.Artifact != "" || r.Revision != ""
	assertion := r.Assertion != ""
	activity := r.Activity != ""
	external := r.URL != "" || r.Retrieval != ""
	families := 0
	for _, present := range []bool{content, located, assertion, activity, external} {
		if present {
			families++
		}
	}
	if families != 1 {
		return ""
	}
	switch {
	case content:
		return FormContent
	case assertion:
		return FormAssertion
	case activity:
		return FormActivity
	case external:
		return FormExternal
	case r.Path != "":
		return FormPath
	case r.Occurrence != "":
		return FormOccurrence
	case r.Row != "" && r.Column != "":
		return FormCell
	case r.Row != "":
		return FormRow
	case r.Column != "":
		return FormColumn
	}
	return FormSnapshot
}

func (r Ref) fields() []string {
	return []string{r.Artifact, r.Revision, r.Schema, r.Row, r.Column, r.Occurrence, r.Digest, r.Assertion, r.Activity, r.URL, r.Retrieval}
}

// Weight approximates resident size, for traversal work budgets.
func (r Ref) Weight() int {
	total := len(r.Path) + 16
	for _, field := range r.fields() {
		total += len(field)
	}
	return total
}

func validDigest(value string) bool {
	hex, ok := strings.CutPrefix(value, "sha256:")
	if !ok || len(hex) != 64 {
		return false
	}
	for _, c := range hex {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Validate checks shape only: exactly one known form with the companions that
// make it exact. Whether the target exists is Resolver's job.
func (r Ref) Validate() error {
	if r.Version != 0 && r.Version != GrammarVersion {
		return fmt.Errorf("provenance: unsupported reference grammar version %d", r.Version)
	}
	for _, v := range r.fields() {
		if len(v) > 1024 {
			return errors.New("provenance: reference field limit")
		}
	}
	if len(r.Path) > 4096 {
		return errors.New("provenance: reference path limit")
	}
	form := r.Form()
	if form == "" {
		if r == (Ref{Version: r.Version}) {
			return errors.New("provenance: artifact or revision required")
		}
		return errors.New("provenance: reference mixes forms")
	}
	if r.Offset < 0 || r.Length < 0 || r.Offset != 0 && r.Length == 0 {
		return errors.New("provenance: range requires offset >= 0 and length > 0")
	}
	if r.hasRange() && form != FormContent && form != FormPath {
		return errors.New("provenance: range requires digest or path")
	}
	switch form {
	case FormSnapshot, FormRow, FormCell, FormColumn:
	default:
		if r.Schema != "" {
			return errors.New("provenance: schema only qualifies table references")
		}
	}
	switch form {
	case FormRow, FormCell, FormColumn:
		if r.Artifact == "" || r.Revision == "" {
			return errors.New("provenance: detailed reference requires artifact and revision")
		}
		if r.Column != "" && r.Schema == "" {
			return errors.New("provenance: column reference requires schema")
		}
	case FormContent:
		if !validDigest(r.Digest) {
			return errors.New("provenance: invalid content digest")
		}
	case FormOccurrence:
		if r.Artifact != "" || r.Revision != "" || r.Detailed() {
			return errors.New("provenance: occurrence reference cannot also name a snapshot or selector")
		}
	case FormPath:
		if (r.Artifact != "") == (r.Occurrence != "") {
			return errors.New("provenance: path requires exactly one of artifact or occurrence")
		}
		if r.Occurrence != "" && r.Revision != "" || r.Detailed() {
			return errors.New("provenance: path reference mixes forms")
		}
		if err := validPath(r.Path); err != nil {
			return err
		}
	case FormExternal:
		if r.URL == "" || r.Retrieval == "" {
			return errors.New("provenance: external reference requires url and retrieval identity")
		}
	}
	return nil
}

func validPath(path string) error {
	if strings.HasPrefix(path, "/") || strings.ContainsAny(path, "\\\x00") {
		return errors.New("provenance: invalid reference path")
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return errors.New("provenance: invalid reference path")
		}
	}
	return nil
}

// Resolver checks that a reference's target exists. Each layer supplies what it
// can reach; a nil function makes that form ErrUnsupportedForm, so nothing is
// stored merely because the layer could not look.
type Resolver struct {
	Revision func(ctx context.Context, revision, artifact string) error
	Artifact func(ctx context.Context, artifact string) error
	// Occurrence returns the artifact the occurrence publishes.
	Occurrence func(ctx context.Context, occurrence string) (artifact string, err error)
	// Content returns the size of bare content, or -1 if it has no single byte stream.
	Content func(ctx context.Context, digest string) (size int64, err error)
	// File returns the size of a regular file within an artifact.
	File      func(ctx context.Context, artifact, path string) (size int64, err error)
	Selector  func(ctx context.Context, ref Ref) error
	Assertion func(ctx context.Context, assertion string) error
	Activity  func(ctx context.Context, activity string) error
}

func requireRange(ref Ref, size int64) error {
	if !ref.hasRange() {
		return nil
	}
	if size < 0 {
		return errors.New("provenance: range requires a single byte stream")
	}
	if ref.Offset > size || ref.Length > size-ref.Offset {
		return errors.New("provenance: range exceeds content size")
	}
	return nil
}

func unsupported(form string) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedForm, form)
}

// Check validates shape, then the target of whichever form the reference has.
func (v Resolver) Check(ctx context.Context, ref Ref) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	switch form := ref.Form(); form {
	case FormContent:
		if v.Content == nil {
			return unsupported(form)
		}
		size, err := v.Content(ctx, ref.Digest)
		if err != nil {
			return err
		}
		return requireRange(ref, size)
	case FormOccurrence:
		if v.Occurrence == nil {
			return unsupported(form)
		}
		_, err := v.Occurrence(ctx, ref.Occurrence)
		return err
	case FormPath:
		return v.checkPath(ctx, ref)
	case FormAssertion:
		if v.Assertion == nil {
			return unsupported(form)
		}
		return v.Assertion(ctx, ref.Assertion)
	case FormActivity:
		if v.Activity == nil {
			return unsupported(form)
		}
		return v.Activity(ctx, ref.Activity)
	case FormExternal:
		return nil // An external snapshot is opaque: its shape is all that can be checked here.
	}
	return v.checkSnapshot(ctx, ref)
}

func (v Resolver) checkPath(ctx context.Context, ref Ref) error {
	artifact := ref.Artifact
	if ref.Occurrence != "" {
		if v.Occurrence == nil {
			return unsupported(FormPath)
		}
		var err error
		if artifact, err = v.Occurrence(ctx, ref.Occurrence); err != nil {
			return err
		}
	}
	if v.File == nil {
		return unsupported(FormPath)
	}
	size, err := v.File(ctx, artifact, ref.Path)
	if err != nil {
		return err
	}
	if ref.Revision != "" {
		if err := v.checkRevision(ctx, ref); err != nil {
			return err
		}
	}
	return requireRange(ref, size)
}

func (v Resolver) checkRevision(ctx context.Context, ref Ref) error {
	if v.Revision == nil {
		return unsupported(FormSnapshot)
	}
	return v.Revision(ctx, ref.Revision, ref.Artifact)
}

func (v Resolver) checkSnapshot(ctx context.Context, ref Ref) error {
	if ref.Revision != "" {
		if err := v.checkRevision(ctx, ref); err != nil {
			return err
		}
	}
	for _, id := range []string{ref.Artifact, ref.Schema} {
		if id == "" {
			continue
		}
		if v.Artifact == nil {
			return unsupported(FormSnapshot)
		}
		if err := v.Artifact(ctx, id); err != nil {
			return err
		}
	}
	if !ref.Detailed() {
		return nil
	}
	if v.Selector == nil {
		return unsupported(ref.Form())
	}
	return v.Selector(ctx, ref)
}

const (
	Value       = "value"
	Predicate   = "predicate"
	Control     = "control"
	Complete    = "complete"
	Incomplete  = "incomplete"
	Unsupported = "unsupported"
)

// Dependency keeps value and selection/control ancestry distinct.
type Dependency struct {
	Ref  Ref    `json:"ref"`
	Role string `json:"role"`
}

// Mapping is one NDJSON evidence record; a shared output can have many inputs.
type Mapping struct {
	Expression string       `json:"expression,omitempty"`
	Output     Ref          `json:"output"`
	Inputs     []Dependency `json:"inputs"`
}

// Coverage is evidence scope, not a claim that a bounded query returned all edges.
type Coverage struct {
	Rows       string   `json:"rows"`
	Columns    string   `json:"columns"`
	Values     string   `json:"values"`
	Predicates string   `json:"predicates"`
	Reasons    []string `json:"reasons,omitempty"`
}

func (c Coverage) Validate() error {
	for _, v := range []string{c.Rows, c.Columns, c.Values, c.Predicates} {
		if v != Complete && v != Incomplete && v != Unsupported {
			return errors.New("provenance: coverage must be complete, incomplete or unsupported")
		}
	}
	if len(c.Reasons) > 64 {
		return errors.New("provenance: reasons limit")
	}
	return nil
}
func (m Mapping) Validate() error {
	if len(m.Expression) > 65536 {
		return errors.New("provenance: expression size limit")
	}
	if err := m.Output.Validate(); err != nil {
		return err
	}
	if !m.Output.Detailed() {
		return errors.New("provenance: mapping output must identify a row or column")
	}
	if len(m.Inputs) > 10000 {
		return errors.New("provenance: dependency limit")
	}
	for _, d := range m.Inputs {
		if err := d.Ref.Validate(); err != nil {
			return err
		}
		if d.Role != Value && d.Role != Predicate && d.Role != Control {
			return errors.New("provenance: invalid dependency role")
		}
	}
	return nil
}
