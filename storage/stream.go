package storage

import (
	"context"
	"errors"
	"github.com/craggy-io/artifact-services/observability"
	"io"
)

// readContext cannot interrupt a blocked reader. Bytes returned before noticing
// cancellation remain counted; cancellation is preserved alongside source errors.
func readContext(ctx context.Context, source io.Reader, p []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n, err := source.Read(p)
	if canceled := ctx.Err(); canceled != nil {
		err = errors.Join(err, canceled)
	}
	return n, err
}

type streamReader struct {
	ctx  context.Context
	base io.Reader
	span *observability.Span
}

func (r *streamReader) Read(p []byte) (int, error) {
	n, err := readContext(r.ctx, r.base, p)
	r.span.AddBytes(int64(n))
	return n, err
}

type contextWriter struct {
	ctx  context.Context
	dest io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.dest.Write(p)
}

// Readers require serial Read/Close calls. EOF and errors are updates; only
// Close finalizes, preserving close failures and avoiding duplicate terminal events.
type blobReader struct {
	streamReader
	closer      io.Closer
	closed, eof bool
	closeErr    error
	terminal    error
}

func (r *blobReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.terminal != nil {
		return 0, r.terminal
	}
	n, err := r.streamReader.Read(p)
	if err == io.EOF {
		if !r.eof {
			r.eof = true
			r.span.Count("eof", 1)
		}
	} else if err != nil {
		r.terminal = err
		r.span.RecordError(err)
	}
	return n, err
}
func (r *blobReader) Close() error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.closeErr = r.closer.Close()
	r.span.Count("closed", 1)
	if !r.eof {
		r.span.Count("early_closed", 1)
	}
	r.span.End(r.closeErr)
	return r.closeErr
}

func startOperation(ctx context.Context, operation, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "storage", Operation: operation}, key)
}
