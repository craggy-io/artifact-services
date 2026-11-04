package journal

// Postings is an in-memory inverted index from an exact key to ascending record
// sequences, rebuilt from the log on open. Callers provide their own locking.
type Postings map[string][]uint64

// Add records a sequence. Commit order makes the lists ascending.
func (p Postings) Add(key string, sequence uint64) { p[key] = append(p[key], sequence) }

// Snapshot returns a list safe to use outside the caller's lock: appends after
// this call do not change what the returned slice can see.
func (p Postings) Snapshot(key string) []uint64 {
	list := p[key]
	return list[:len(list):len(list)]
}

// IndexKey builds a posting key from parts that may contain any bytes except NUL.
func IndexKey(parts ...string) string {
	total := len(parts)
	for _, part := range parts {
		total += len(part)
	}
	key := make([]byte, 0, total)
	for i, part := range parts {
		if i > 0 {
			key = append(key, 0)
		}
		key = append(key, part...)
	}
	return string(key)
}
