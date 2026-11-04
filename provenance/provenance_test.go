package provenance

import (
	"strings"
	"testing"
)

func TestReferenceAndCoverageContracts(t *testing.T) {
	for _, r := range []Ref{{}, {Row: "r"}, {Artifact: "a", Row: "r"}, {Artifact: "a", Revision: "v", Column: "c"}} {
		if r.Validate() == nil {
			t.Fatalf("accepted invalid ref: %+v", r)
		}
	}
	if err := (Ref{Artifact: "a", Revision: "v", Schema: "s", Row: "r", Column: "c"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if (Coverage{Rows: Complete, Columns: Complete, Values: Complete, Predicates: Unsupported}).Validate() != nil {
		t.Fatal("explicit unsupported status rejected")
	}
	if (Coverage{}).Validate() == nil {
		t.Fatal("unspecified coverage accepted")
	}
	m := Mapping{Output: Ref{Artifact: "a", Revision: "v", Row: "r"}, Expression: strings.Repeat("x", 65537)}
	if m.Validate() == nil {
		t.Fatal("expression limit ignored")
	}
}
