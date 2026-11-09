package journal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/craggy-io/artifact-services/backend"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/crashpoint"
	"github.com/craggy-io/artifact-services/internal/durable"
)

const (
	// Format identifies the log layout. Open refuses any other value; the upgrade
	// policy is documented in docs/stability.md.
	Format = "ASJOURNAL1\n"

	logName    = "journal.log"
	configName = "config.json"
	leaseName  = ".lease"

	frameHeader = 8
	// A record's envelope adds its sequence, key (up to 1 KiB) and JSON framing.
	maxFrameSize = maxRecord + 4096

	// DefaultMaxBytes and DefaultMaxRecords apply when Options leaves a limit zero.
	DefaultMaxBytes   = int64(64 << 30)
	DefaultMaxRecords = 20_000_000
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// fileLog is the local backend: a bounded, append-only file of checksummed frames
// under an exclusive writer lease. Only keys and record offsets stay resident;
// values are read back from the file on demand. The lease makes this process the
// only writer, so its Watch never fires.
type fileLog struct {
	mu       sync.RWMutex
	root     string
	lease    *lease
	log      *os.File
	options  Options
	index    map[string]uint64
	offsets  []int64
	end      int64
	bytes    int64
	closed   bool
	poisoned error
}

var _ backend.Log = (*fileLog)(nil)

func openFile(root string, o Options, config []byte) (result *fileLog, err error) {
	if root == "" {
		return nil, errors.New("journal: empty root")
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = DefaultMaxBytes
	}
	if o.MaxRecords == 0 {
		o.MaxRecords = DefaultMaxRecords
	}
	if o.MaxBytes < 1 || o.MaxRecords < 1 {
		return nil, errors.New("journal: invalid limits")
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	held, err := acquireLease(filepath.Join(root, leaseName), root)
	if err != nil {
		return nil, err
	}
	s := &fileLog{root: root, lease: held, options: o, index: map[string]uint64{}}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	if err = s.bindConfig(config); err != nil {
		return nil, err
	}
	if err = s.rejectUnknownFiles(); err != nil {
		return nil, err
	}
	if err = s.openLog(); err != nil {
		return nil, err
	}
	if err = s.replay(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *fileLog) bindConfig(config []byte) error {
	path := filepath.Join(s.root, configName)
	existing, err := readBounded(path, 8<<20)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return publish(path, config)
	case err != nil:
		return err
	case !bytes.Equal(config, existing):
		return ErrConfig
	}
	return nil
}

func (s *fileLog) rejectUnknownFiles() error {
	files, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, ".tmp-") {
			// A crash during config publication can strand a temporary file.
			if err := os.Remove(filepath.Join(s.root, name)); err != nil {
				return err
			}
			continue
		}
		if name != configName && name != leaseName && name != logName {
			return fmt.Errorf("%w: unexpected file %s", ErrFormat, name)
		}
	}
	return nil
}

func (s *fileLog) openLog() error {
	path := filepath.Join(s.root, logName)
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if errors.Is(err, os.ErrNotExist) {
		if f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600); err != nil {
			return err
		}
		s.log = f
		return s.initLog(path)
	}
	if err != nil {
		return err
	}
	s.log = f
	header := make([]byte, len(Format))
	n, err := f.ReadAt(header, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(Format) {
		// A crash before the header finished leaves no commits to lose.
		if !strings.HasPrefix(Format, string(header[:n])) {
			return ErrFormat
		}
		if err := f.Truncate(0); err != nil {
			return err
		}
		return s.initLog(path)
	}
	if string(header) != Format {
		return fmt.Errorf("%w: found %q, supported %q", ErrFormat, header, Format)
	}
	return nil
}

func (s *fileLog) initLog(path string) error {
	if _, err := s.log.WriteAt([]byte(Format), 0); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	return durable.Published(path, "")
}

// replay rebuilds the key index with one sequential pass. A damaged final frame
// is a commit that never finished, so it is truncated; damage anywhere earlier
// is corruption and is reported rather than silently dropping later records.
func (s *fileLog) replay() error {
	info, err := s.log.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	offset := int64(len(Format))
	reader := bufio.NewReaderSize(io.NewSectionReader(s.log, offset, size-offset), 1<<20)
	next := uint64(1)
	for offset < size {
		payload, frame, err := readFrame(reader, size-offset)
		if errors.Is(err, errTorn) {
			if err := s.log.Truncate(offset); err != nil {
				return err
			}
			if err := s.log.Sync(); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		entry, err := decodeEntry(payload)
		if err != nil {
			return err
		}
		if entry.Sequence != next {
			return fmt.Errorf("%w: sequence %d at position %d", ErrCorrupt, entry.Sequence, next)
		}
		if _, ok := s.index[entry.Key]; ok {
			return fmt.Errorf("%w: %w", ErrCorrupt, ErrConflict)
		}
		s.index[entry.Key] = entry.Sequence
		s.offsets = append(s.offsets, offset)
		s.bytes += frame
		offset += frame
		next++
	}
	s.end = offset
	return nil
}

var errTorn = errors.New("journal: torn frame")

// readFrame returns the payload and total frame size. remaining distinguishes a
// torn tail (the frame runs past end of file, or is the last frame and fails its
// checksum) from corruption with committed records still following it.
func readFrame(r io.Reader, remaining int64) ([]byte, int64, error) {
	var header [frameHeader]byte
	if remaining < frameHeader {
		return nil, 0, errTorn
	}
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}
	size := int64(binary.LittleEndian.Uint32(header[:4]))
	if size > maxFrameSize || size == 0 {
		// A garbage length is only explicable as a tear if nothing could follow it.
		if remaining-frameHeader <= maxFrameSize {
			return nil, 0, errTorn
		}
		return nil, 0, fmt.Errorf("%w: frame length %d", ErrCorrupt, size)
	}
	frame := frameHeader + size
	if frame > remaining {
		return nil, 0, errTorn
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(header[4:8]) {
		if frame == remaining {
			return nil, 0, errTorn
		}
		return nil, 0, fmt.Errorf("%w: checksum", ErrCorrupt)
	}
	return payload, frame, nil
}

func encodeFrame(payload []byte) []byte {
	frame := make([]byte, frameHeader+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.Checksum(payload, castagnoli))
	copy(frame[frameHeader:], payload)
	return frame
}

// committing marks a context passed to prepare: Append already holds s.mu, so reads
// made with it must not take the lock again.
type committing struct{ log *fileLog }

// readLock takes the read lock unless ctx comes from this log's own prepare call.
func (s *fileLog) readLock(ctx context.Context) (unlock func()) {
	if held, _ := ctx.Value(committing{}).(*fileLog); held == s {
		return func() {}
	}
	s.mu.RLock()
	return s.mu.RUnlock
}

func (s *fileLog) Latest(ctx context.Context) (uint64, error) {
	defer s.readLock(ctx)()
	if s.closed {
		return 0, ErrClosed
	}
	return uint64(len(s.offsets)), nil
}

func (s *fileLog) readAt(offset int64) (Entry, error) {
	var header [frameHeader]byte
	if _, err := s.log.ReadAt(header[:], offset); err != nil {
		return Entry{}, readError(err)
	}
	size := binary.LittleEndian.Uint32(header[:4])
	if size == 0 || size > maxFrameSize {
		return Entry{}, ErrCorrupt
	}
	payload := make([]byte, size)
	if _, err := s.log.ReadAt(payload, offset+frameHeader); err != nil {
		return Entry{}, readError(err)
	}
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(header[4:8]) {
		return Entry{}, fmt.Errorf("%w: checksum", ErrCorrupt)
	}
	return decodeTrusted(payload)
}

func readError(err error) error {
	if errors.Is(err, os.ErrClosed) {
		return ErrClosed
	}
	return err
}

func (s *fileLog) Get(ctx context.Context, key string) (Entry, bool, error) {
	unlock := s.readLock(ctx)
	sequence, ok := s.index[key]
	closed := s.closed
	var offset int64
	if ok {
		offset = s.offsets[sequence-1]
	}
	log := s.log
	unlock()
	if closed || log == nil {
		return Entry{}, false, ErrClosed
	}
	if !ok {
		return Entry{}, false, nil
	}
	entry, err := s.readAt(offset)
	if err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

func (s *fileLog) At(ctx context.Context, sequence uint64) (Entry, error) {
	unlock := s.readLock(ctx)
	closed := s.closed
	valid := sequence >= 1 && sequence <= uint64(len(s.offsets))
	var offset int64
	if valid {
		offset = s.offsets[sequence-1]
	}
	unlock()
	if closed {
		return Entry{}, ErrClosed
	}
	if !valid {
		return Entry{}, fmt.Errorf("%w: no sequence %d", ErrCorrupt, sequence)
	}
	return s.readAt(offset)
}

// Scan reads the log sequentially with one resident record at a time.
func (s *fileLog) Scan(ctx context.Context, after, until uint64, visit func(Entry) error) error {
	unlock := s.readLock(ctx)
	closed := s.closed
	latest := uint64(len(s.offsets))
	if until > latest {
		until = latest
	}
	var start, stop int64
	if !closed && after < until {
		start = s.offsets[after]
		stop = s.end
		if until < latest {
			stop = s.offsets[until]
		}
	}
	unlock()
	if closed {
		return ErrClosed
	}
	if after >= until {
		return nil
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(s.log, start, stop-start), 1<<20)
	remaining := stop - start
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		payload, frame, err := readFrame(reader, remaining)
		if err != nil {
			if errors.Is(err, errTorn) {
				return fmt.Errorf("%w: committed frame", ErrCorrupt)
			}
			return readError(err)
		}
		entry, err := decodeTrusted(payload)
		if err != nil {
			return err
		}
		if err := visit(entry); err != nil {
			return err
		}
		remaining -= frame
	}
	return nil
}

func (s *fileLog) Append(ctx context.Context, key string, data []byte, prepare func(context.Context, uint64) error) (Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Entry{}, false, ErrClosed
	}
	if s.poisoned != nil {
		return Entry{}, false, s.poisoned
	}
	if sequence, ok := s.index[key]; ok {
		old, err := s.readAt(s.offsets[sequence-1])
		if err != nil {
			return Entry{}, false, err
		}
		return old, true, nil
	}
	if err := prepare(context.WithValue(ctx, committing{}, s), uint64(len(s.offsets))); err != nil {
		return Entry{}, false, err
	}
	e := Entry{Sequence: uint64(len(s.offsets)) + 1, Key: key, Data: data}
	encoded, err := canon.Marshal(e)
	if err != nil {
		return Entry{}, false, err
	}
	frame := encodeFrame(encoded)
	if len(encoded) > maxFrameSize || s.bytes+int64(len(frame)) > s.options.MaxBytes || len(s.offsets) >= s.options.MaxRecords {
		return Entry{}, false, ErrCapacity
	}
	if err = ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	if err = s.writeFrame(frame); err != nil {
		return Entry{}, false, &CommitError{Err: err}
	}
	s.index[key] = e.Sequence
	s.offsets = append(s.offsets, s.end)
	s.end += int64(len(frame))
	s.bytes += int64(len(frame))
	return e, false, nil
}

// Watch never fires: the lease makes this process the root's only writer.
func (s *fileLog) Watch(func()) func() { return func() {} }

// writeFrame leaves the log at its previous length on failure, so an error can
// honestly report the record absent. If that restoration itself fails the log
// refuses further commits, because the on-disk tail is no longer known.
func (s *fileLog) writeFrame(frame []byte) error {
	crashpoint.Hit(crashpoint.JournalBeforeWrite)
	if crashpoint.Armed(crashpoint.JournalTornWrite) {
		if _, err := s.log.WriteAt(frame[:len(frame)/2], s.end); err != nil {
			return err
		}
		if err := s.log.Sync(); err != nil {
			return err
		}
	}
	crashpoint.Hit(crashpoint.JournalTornWrite)
	_, err := s.log.WriteAt(frame, s.end)
	crashpoint.Hit(crashpoint.JournalBeforeSync)
	if err == nil && !s.options.Relaxed {
		err = s.log.Sync()
	}
	if err == nil {
		return nil
	}
	if restore := s.log.Truncate(s.end); restore != nil {
		s.poisoned = fmt.Errorf("journal: log tail unknown after failed commit: %w", errors.Join(err, restore))
		return s.poisoned
	}
	return err
}

// publish durably creates a new immutable file, for the config which is written
// once per root. The log itself is the only other file written by this package.
func publish(path string, data []byte) (result error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, f.Close())
		}
		result = errors.Join(result, os.Remove(f.Name()))
	}()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	closed = true
	if err != nil {
		return err
	}
	if err = os.Link(f.Name(), path); err != nil {
		return err
	}
	return durable.Published(path, "")
}

// Close releases the file and the lease. It is retryable: a failed step is
// attempted again by a later Close.
func (s *fileLog) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var result error
	if s.log != nil {
		if err := s.log.Close(); err == nil || errors.Is(err, os.ErrClosed) {
			s.log = nil
		} else {
			result = errors.Join(result, err)
		}
	}
	if s.lease != nil {
		if err := s.lease.release(); err == nil {
			s.lease = nil
		} else {
			result = errors.Join(result, err)
		}
	}
	return result
}

// decodeEntry strictly validates a record for replay on open.
func decodeEntry(data []byte) (Entry, error) {
	canonical, err := canon.Normalize(data)
	if err != nil {
		return Entry{}, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if !bytes.Equal(data, canonical) {
		return Entry{}, fmt.Errorf("%w: noncanonical record", ErrCorrupt)
	}
	var e Entry
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&e); err != nil {
		return Entry{}, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if err = requireKey(e.Key); err != nil {
		return Entry{}, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if len(e.Data) == 0 || !json.Valid(e.Data) {
		return Entry{}, fmt.Errorf("%w: invalid data", ErrCorrupt)
	}
	return e, nil
}

// decodeTrusted decodes a checksummed record already validated by replay or by
// this process's own Commit.
func decodeTrusted(data []byte) (Entry, error) {
	var e Entry
	if err := json.Unmarshal(data, &e); err != nil {
		return Entry{}, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return e, nil
}

func readBounded(path string, limit int64) (data []byte, result error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, f.Close()) }()
	data, err = io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("journal: file size limit")
	}
	return data, nil
}
