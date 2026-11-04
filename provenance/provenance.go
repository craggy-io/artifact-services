// Package provenance defines neutral, version-qualified sub-object evidence.
package provenance

import "errors"

// Ref keeps identity separate from display names and offsets. A bare Revision is
// a whole revision reference. Row/Column references require Artifact and Revision;
// Column additionally requires the exact Schema artifact.
type Ref struct {
	Artifact string `json:"artifact,omitempty"`
	Revision string `json:"revision,omitempty"`
	Schema   string `json:"schema,omitempty"`
	Row      string `json:"row,omitempty"`
	Column   string `json:"column,omitempty"`
}

func (r Ref) Detailed() bool { return r.Row != "" || r.Column != "" }
func (r Ref) Validate() error {
	if r.Artifact == "" && r.Revision == "" {
		return errors.New("provenance: artifact or revision required")
	}
	if r.Detailed() && (r.Artifact == "" || r.Revision == "") {
		return errors.New("provenance: detailed reference requires artifact and revision")
	}
	if r.Column != "" && r.Schema == "" {
		return errors.New("provenance: column reference requires schema")
	}
	for _, v := range []string{r.Artifact, r.Revision, r.Schema, r.Row, r.Column} {
		if len(v) > 1024 {
			return errors.New("provenance: reference field limit")
		}
	}
	return nil
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
