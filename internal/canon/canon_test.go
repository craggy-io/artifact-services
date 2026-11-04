package canon

import (
	"bytes"
	"strings"
	"testing"
)

func TestCanonical(t *testing.T) {
	a, e := Normalize([]byte(`{"z":1.00,"a":[1e2,-0]}`))
	if e != nil || string(a) != `{"a":[100,0],"z":1}` {
		t.Fatal(string(a), e)
	}
	b, e := Normalize(a)
	if e != nil || !bytes.Equal(a, b) {
		t.Fatal(string(b), e)
	}
	for _, bad := range []string{`{"a":1,"a":2}`, `1e999999999`, `{} {}`, `[1,]`} {
		if _, e := Normalize([]byte(bad)); e == nil {
			t.Fatal(bad)
		}
	}
}
func TestCanonicalLargeNumbersRemainReadable(t *testing.T) {
	for _, raw := range []string{"1e1024", "-1e4096", "1e-4096"} {
		a, err := Normalize([]byte(raw))
		if err != nil {
			t.Fatal(raw, err)
		}
		b, err := Normalize(a)
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("not closed: %s: %v", raw, err)
		}
	}
	if _, err := Normalize([]byte{34, 255, 34}); err == nil {
		t.Fatal("invalid UTF8 accepted")
	}
}
func TestExpansionBudget(t *testing.T) {
	data := []byte("[" + strings.Repeat("1e4096,", 2100) + "0]")
	if _, err := Normalize(data); err == nil {
		t.Fatal("expansion budget not enforced")
	}
}
