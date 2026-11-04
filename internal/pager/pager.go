// Package pager provides bounded replayable process-local snapshot cursors.
package pager

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/craggy-io/artifact-services/internal/canon"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrCursor = errors.New("pager: invalid, expired or mismatched cursor")
var ErrCapacity = errors.New("pager: snapshot capacity exceeded")

type session[T any] struct {
	query   [32]byte
	values  []T
	expires time.Time
	bytes   int
}
type Cache[T any] struct {
	mu       sync.Mutex
	secret   [32]byte
	sessions map[string]session[T]
	bytes    int
	now      func() time.Time
}

func New[T any]() *Cache[T] {
	c := &Cache[T]{sessions: map[string]session[T]{}, now: time.Now}
	if _, err := rand.Read(c.secret[:]); err != nil {
		panic(err)
	}
	return c
}

// Page binds all normalized query fields and limit to a 15-minute snapshot.
// Builds happen outside the lock. Snapshots are bounded to 16 MiB each, 64 MiB
// total, and 128 sessions. Returned pages are deep JSON copies, not cache aliases.
func (c *Cache[T]) Page(query any, cursor string, limit int, build func() ([]T, error)) ([]T, string, error) {
	if limit < 0 || limit > 1000 {
		return nil, "", errors.New("pager: invalid page size")
	}
	if limit == 0 {
		limit = 100
	}
	q, err := canon.Marshal(struct {
		Query any
		Limit int
	}{query, limit})
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(q)
	var id string
	offset := 0
	var fresh []T
	size := 0
	if cursor == "" {
		fresh, err = build()
		if err != nil {
			return nil, "", err
		}
		encoded, e := json.Marshal(fresh)
		if e != nil {
			return nil, "", e
		}
		size = len(encoded)
		if size > 16<<20 {
			return nil, "", ErrCapacity
		}
		if e = json.Unmarshal(encoded, &fresh); e != nil {
			return nil, "", e
		}
		var random [16]byte
		if _, e = rand.Read(random[:]); e != nil {
			return nil, "", e
		}
		id = hex.EncodeToString(random[:])
	} else {
		parts := strings.Split(cursor, ".")
		if len(parts) != 3 {
			return nil, "", ErrCursor
		}
		id = parts[0]
		offset, err = strconv.Atoi(parts[1])
		if err != nil || offset < 0 {
			return nil, "", ErrCursor
		}
		mac, err := hex.DecodeString(parts[2])
		if err != nil || !hmac.Equal(mac, c.mac(parts[0]+"."+parts[1])) {
			return nil, "", ErrCursor
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for k, s := range c.sessions {
		if !now.Before(s.expires) {
			delete(c.sessions, k)
			c.bytes -= s.bytes
		}
	}
	var s session[T]
	if cursor == "" {
		s = session[T]{query: hash, values: fresh, expires: now.Add(15 * time.Minute), bytes: size}
		if len(fresh) > limit {
			if len(c.sessions) >= 128 || c.bytes+size > 64<<20 {
				return nil, "", ErrCapacity
			}
			c.sessions[id] = s
			c.bytes += size
		}
	}
	if cursor != "" {
		var ok bool
		s, ok = c.sessions[id]
		if !ok || s.query != hash || offset > len(s.values) {
			return nil, "", ErrCursor
		}
	}
	end := offset + limit
	if end > len(s.values) {
		end = len(s.values)
	}
	encoded, err := json.Marshal(s.values[offset:end])
	if err != nil {
		return nil, "", err
	}
	var page []T
	if err = json.Unmarshal(encoded, &page); err != nil {
		return nil, "", err
	}
	next := ""
	if end < len(s.values) {
		prefix := id + "." + strconv.Itoa(end)
		next = prefix + "." + hex.EncodeToString(c.mac(prefix))
	}
	return page, next, nil
}
func (c *Cache[T]) mac(s string) []byte {
	m := hmac.New(sha256.New, c.secret[:])
	m.Write([]byte(s))
	return m.Sum(nil)
}

// Position seals a resume position bound to a query. It suits immutable data,
// where no snapshot needs caching: the position alone resumes the listing.
func (c *Cache[T]) Position(query any, position string) (string, error) {
	hash, err := queryHash(query)
	if err != nil {
		return "", err
	}
	encoded := hex.EncodeToString([]byte(position))
	return "p." + encoded + "." + hex.EncodeToString(c.mac(hash+"."+encoded)), nil
}

// OpenPosition returns the position sealed by Position, or ErrCursor if the
// cursor is invalid, came from another service, or was made for another query.
func (c *Cache[T]) OpenPosition(query any, cursor string) (string, error) {
	hash, err := queryHash(query)
	if err != nil {
		return "", err
	}
	parts := strings.Split(cursor, ".")
	if len(parts) != 3 || parts[0] != "p" {
		return "", ErrCursor
	}
	mac, err := hex.DecodeString(parts[2])
	if err != nil || !hmac.Equal(mac, c.mac(hash+"."+parts[1])) {
		return "", ErrCursor
	}
	position, err := hex.DecodeString(parts[1])
	if err != nil {
		return "", ErrCursor
	}
	return string(position), nil
}

func queryHash(query any) (string, error) {
	encoded, err := canon.Marshal(query)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
