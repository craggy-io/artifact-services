// Package canon defines the library's v1 canonical JSON representation.
package canon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Marshal canonicalizes JSON, rejecting duplicate members and non-finite numbers.
func Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return Normalize(b)
}

// MaxBytes bounds canonical JSON input and expanded output.
const MaxBytes = 8 << 20

type budget struct{ used int }

func (b *budget) add(n int) error {
	b.used += n
	if b.used > MaxBytes {
		return errors.New("canonical JSON: output budget exceeded")
	}
	return nil
}
func (b *budget) encoded(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.add(len(raw))
}

// Normalize sorts object keys, normalizes exact finite decimal numbers and rejects
// duplicate members. JSON strings use encoding/json's deterministic escaping.
func Normalize(data []byte) ([]byte, error) {
	if len(data) > MaxBytes {
		return nil, errors.New("canonical JSON: input budget exceeded")
	}
	if !utf8.Valid(data) {
		return nil, errors.New("canonical JSON: invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	v, err := value(d, 0, &budget{})
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("canonical JSON: trailing content")
	}
	return json.Marshal(v)
}
func value(d *json.Decoder, depth int, b *budget) (any, error) {
	if depth > 64 {
		return nil, errors.New("canonical JSON: nesting limit")
	}
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t := token.(type) {
	case json.Delim:
		switch t {
		case '{':
			if err := b.add(2); err != nil {
				return nil, err
			}
			m := map[string]any{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				key, ok := k.(string)
				if !ok {
					return nil, errors.New("object key")
				}
				if err := b.encoded(key); err != nil {
					return nil, err
				}
				punctuation := 1
				if len(m) > 0 {
					punctuation++
				}
				if err := b.add(punctuation); err != nil {
					return nil, err
				}
				if _, exists := m[key]; exists {
					return nil, fmt.Errorf("duplicate member %q", key)
				}
				v, err := value(d, depth+1, b)
				if err != nil {
					return nil, err
				}
				m[key] = v
			}
			if _, err = d.Token(); err != nil {
				return nil, err
			}
			return m, nil
		case '[':
			if err := b.add(2); err != nil {
				return nil, err
			}
			a := []any{}
			for d.More() {
				if len(a) > 0 {
					if err := b.add(1); err != nil {
						return nil, err
					}
				}
				v, err := value(d, depth+1, b)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			if _, err = d.Token(); err != nil {
				return nil, err
			}
			return a, nil
		}
		return nil, errors.New("unexpected delimiter")
	case json.Number:
		raw := string(t)
		if len(raw) > 8192 {
			return nil, errors.New("number length limit")
		}
		if i := strings.IndexAny(raw, "eE"); i >= 0 {
			exp, e := strconv.Atoi(raw[i+1:])
			if e != nil || exp < -4096 || exp > 4096 {
				return nil, errors.New("number exponent limit")
			}
		}
		r, ok := new(big.Rat).SetString(raw)
		if !ok {
			return nil, errors.New("invalid number")
		}
		// Finite JSON decimals have denominators containing only factors 2 and 5.
		den := new(big.Int).Set(r.Denom())
		two, five := big.NewInt(2), big.NewInt(5)
		counts := [2]int{}
		for i, f := range []*big.Int{two, five} {
			for new(big.Int).Mod(den, f).Sign() == 0 {
				den.Div(den, f)
				counts[i]++
				if counts[i] > 4096 {
					return nil, errors.New("number exponent limit")
				}
			}
		}
		digits := counts[0]
		if counts[1] > digits {
			digits = counts[1]
		}
		s := r.FloatString(digits)
		if strings.Contains(s, ".") {
			s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
		}
		if s == "-0" {
			s = "0"
		}
		if len(s) > 8192 {
			return nil, errors.New("number size limit")
		}
		if err := b.add(len(s)); err != nil {
			return nil, err
		}
		return json.Number(s), nil
	default:
		if err := b.encoded(t); err != nil {
			return nil, err
		}
		return t, nil
	}
}

// IndexedScalar normalizes a value used as an exact-match index key: a string of
// at most 256 bytes, a bool, or a signed 64-bit integer.
func IndexedScalar(raw []byte) ([]byte, error) {
	normalized, err := Normalize(raw)
	if err != nil {
		return nil, err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(normalized))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	switch x := value.(type) {
	case string:
		if len(x) > 256 {
			return nil, errors.New("indexed string exceeds 256 bytes")
		}
	case bool:
	case json.Number:
		if _, err := x.Int64(); err != nil {
			return nil, errors.New("indexed number must be signed int64")
		}
	default:
		return nil, errors.New("indexed value must be string, bool or int64")
	}
	return normalized, nil
}
