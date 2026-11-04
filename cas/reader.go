package cas

import (
	"context"
	"encoding/hex"
	"errors"
	"hash"
	"io"

	"github.com/craggy-io/artifact-services/observability"
)

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

type countedReader struct {
	ctx    context.Context
	source io.Reader
	span   *observability.Span
}

func (r *countedReader) Read(p []byte) (int, error) {
	n, err := readContext(r.ctx, r.source, p)
	r.span.AddBytes(int64(n))
	return n, err
}

type verifiedReader struct {
	ctx              context.Context
	base             io.ReadCloser
	span             *observability.Span
	digest           hash.Hash
	expected         string
	terminal         error
	verified, closed bool
	closeErr         error
}

func (r *verifiedReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.terminal != nil {
		return 0, r.terminal
	}
	n, err := readContext(r.ctx, r.base, p)
	if n > 0 {
		_, _ = r.digest.Write(p[:n])
		r.span.AddBytes(int64(n))
	}
	if err == io.EOF {
		if hex.EncodeToString(r.digest.Sum(nil)) != r.expected {
			err = ErrIntegrity
		} else {
			r.verified = true
			r.span.Count("verified", 1)
		}
	}
	if err != nil {
		r.terminal = err
		if err != io.EOF {
			r.span.RecordError(err)
		}
	}
	return n, err
}
func (r *verifiedReader) Close() error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.closeErr = r.base.Close()
	r.span.Count("closed", 1)
	if !r.verified {
		r.span.Count("unverified_close", 1)
	}
	r.span.End(r.closeErr)
	return r.closeErr
}
