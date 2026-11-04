// Package treeio moves whole trees between a local filesystem and artifacts: a walk
// into a manifest through CAS, and the inverse onto disk with escape safety. Which
// links to follow, what to exclude and how to name an origin are policy, which is why
// this is not core. It fetches nothing over a network: callers fetch a download
// themselves and pass a Reader with its origin.
package treeio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/provenance"
)

// SymlinkPolicy decides what a symbolic link found during a walk does.
type SymlinkPolicy int

const (
	// SymlinkReject fails the ingest. It is the default because a tree with links in
	// it does not round-trip as one.
	SymlinkReject SymlinkPolicy = iota
	// SymlinkSkip omits links.
	SymlinkSkip
	// SymlinkFollowInside stores what a link to a regular file inside the root points
	// to. Links to directories or outside the root still fail.
	SymlinkFollowInside
)

// ErrSymlink reports a link the policy does not accept.
var ErrSymlink = errors.New("treeio: symbolic link rejected by policy")

// ErrEscape reports a stored path that would land outside the destination.
var ErrEscape = errors.New("treeio: path escapes destination")

// Services are the layers ingest writes to. Lineage is optional and records origin.
type Services struct {
	CAS       cas.Store
	Artifacts *artifacts.Service
	Lineage   *lineage.Service
}

// Options configures an ingest. Exclude omits a slash-separated relative path, and a
// directory's whole subtree. Mode decides stored modes, defaulting to the file's
// permission bits, which on Windows carry no executable information. Occurrence
// makes the publication idempotent; empty mints one. Source overrides the origin,
// which otherwise is the file: URL of root with the artifact as retrieval identity.
type Options struct {
	Symlinks   SymlinkPolicy
	Exclude    func(path string, d fs.DirEntry) bool
	Mode       func(path string, info fs.FileInfo) uint32
	Metadata   artifacts.Manifest
	Occurrence artifacts.OccurrenceID
	Name       string
	Source     *provenance.Ref
	MaxMemory  int
	TempDir    string
}

// Ingested reports a published tree.
type Ingested struct {
	Artifact    artifacts.ID
	Occurrence  artifacts.OccurrenceID
	Files, Dirs int64
	Bytes       int64
	Source      provenance.Ref
}

func defaultMode(_ string, info fs.FileInfo) uint32 { return uint32(info.Mode().Perm()) }

func (o Options) mode() func(string, fs.FileInfo) uint32 {
	if o.Mode != nil {
		return o.Mode
	}
	return defaultMode
}

func fileURL(path string) string {
	u := url.URL{Scheme: "file", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}
	return u.String()
}

// Ingest walks root, a directory or single file, storing content through CAS and
// streaming entries into a manifest so that neither is held in memory. A single file
// is stored under its base name. A directory's empty subdirectories are preserved.
func Ingest(ctx context.Context, services Services, root string, options Options) (Ingested, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Ingested{}, err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return Ingested{}, err
	}
	builder := services.Artifacts.NewBuilder(artifacts.BuilderOptions{Metadata: options.Metadata.Metadata, MaxMemory: options.MaxMemory, TempDir: options.TempDir})
	defer builder.Close()
	var out Ingested
	if info.IsDir() {
		err = walkInto(ctx, services, absolute, options, builder, &out)
	} else if info.Mode().IsRegular() {
		err = addFile(ctx, services, absolute, filepath.Base(absolute), info, options, builder, &out)
	} else {
		err = fmt.Errorf("treeio: %s is neither a regular file nor a directory", absolute)
	}
	if err != nil {
		return Ingested{}, err
	}
	if out.Artifact, err = builder.Publish(ctx); err != nil {
		return Ingested{}, err
	}
	source := provenance.Ref{URL: fileURL(absolute), Retrieval: string(out.Artifact)}
	if options.Source != nil {
		source = *options.Source
	}
	return finish(ctx, services, out, source, options)
}

// IngestReader stores one stream, typically a download the caller fetched, as a
// single-file tree named name with the given origin.
func IngestReader(ctx context.Context, services Services, name string, content io.Reader, mode uint32, source provenance.Ref, options Options) (Ingested, error) {
	put, err := services.CAS.Put(ctx, content)
	if err != nil {
		return Ingested{}, err
	}
	var out Ingested
	out.Files, out.Bytes = 1, put.Size
	out.Artifact, err = services.Artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Metadata: options.Metadata.Metadata, Entries: []artifacts.Entry{{Path: name, Content: put.ID, Size: put.Size, Mode: mode}}})
	if err != nil {
		return Ingested{}, err
	}
	return finish(ctx, services, out, source, options)
}

func finish(ctx context.Context, services Services, out Ingested, source provenance.Ref, options Options) (Ingested, error) {
	if err := source.Validate(); err != nil {
		return out, fmt.Errorf("treeio: origin: %w", err)
	}
	if source.Form() != provenance.FormExternal {
		return out, errors.New("treeio: origin must be an external source reference")
	}
	out.Source = source
	occurrence, err := services.Artifacts.RecordOccurrence(ctx, artifacts.OccurrenceRequest{ID: options.Occurrence, Artifact: out.Artifact, Name: options.Name, Origin: source.URL})
	if err != nil {
		return out, err
	}
	out.Occurrence = occurrence.ID
	if services.Lineage != nil {
		edge := lineage.Record{ID: lineage.ID("treeio/origin/" + string(occurrence.ID)), Relationship: &lineage.Relationship{Kind: "treeio.ingested-from", From: provenance.Ref{Occurrence: string(occurrence.ID)}, To: source}}
		if _, err := services.Lineage.Record(ctx, edge); err != nil {
			return out, err
		}
	}
	return out, nil
}

func walkInto(ctx context.Context, services Services, root string, options Options, builder *artifacts.Builder, out *Ingested) error {
	type frame struct {
		relative string
		children int
	}
	// Directories are only worth an entry if nothing below them produced one.
	var stack []*frame
	closeTo := func(relative string) error {
		for len(stack) > 0 {
			top := stack[len(stack)-1]
			if top.relative == "" || relative == top.relative || strings.HasPrefix(relative, top.relative+"/") {
				return nil
			}
			stack = stack[:len(stack)-1]
			if top.children == 0 {
				if err := builder.Add(artifacts.Entry{Path: top.relative, Kind: artifacts.KindDir}); err != nil {
					return err
				}
				out.Dirs++
				if len(stack) > 0 {
					stack[len(stack)-1].children++
				}
			}
		}
		return nil
	}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative == "." {
			stack = append(stack, &frame{})
			return nil
		}
		relative = filepath.ToSlash(relative)
		if options.Exclude != nil && options.Exclude(relative, d) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if err := closeTo(relative); err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			if len(stack) > 0 {
				stack[len(stack)-1].children++
			}
			stack = append(stack, &frame{relative: relative})
			return nil
		case info.Mode()&fs.ModeSymlink != 0:
			followed, target, err := followLink(root, path, options.Symlinks)
			if err != nil || !followed {
				return err
			}
			if err := addFile(ctx, services, target, relative, nil, options, builder, out); err != nil {
				return err
			}
		case info.Mode().IsRegular():
			if err := addFile(ctx, services, path, relative, info, options, builder, out); err != nil {
				return err
			}
		default:
			return fmt.Errorf("treeio: %s is neither a regular file nor a directory", relative)
		}
		if len(stack) > 0 {
			stack[len(stack)-1].children++
		}
		return nil
	})
	if err != nil {
		return err
	}
	return closeTo("\x00")
}

// followLink applies the policy, returning the file to read if the link is followed.
func followLink(root, path string, policy SymlinkPolicy) (bool, string, error) {
	switch policy {
	case SymlinkSkip:
		return false, "", nil
	case SymlinkFollowInside:
		realRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			return false, "", err
		}
		target, err := filepath.EvalSymlinks(path)
		if err != nil {
			return false, "", fmt.Errorf("%w: %s: %v", ErrSymlink, path, err)
		}
		relative, err := filepath.Rel(realRoot, target)
		if err != nil || !filepath.IsLocal(relative) {
			return false, "", fmt.Errorf("%w: %s points outside the root", ErrSymlink, path)
		}
		info, err := os.Stat(target)
		if err != nil {
			return false, "", err
		}
		if !info.Mode().IsRegular() {
			return false, "", fmt.Errorf("%w: %s does not point to a regular file", ErrSymlink, path)
		}
		return true, target, nil
	}
	return false, "", fmt.Errorf("%w: %s", ErrSymlink, path)
}

func addFile(ctx context.Context, services Services, path, relative string, info fs.FileInfo, options Options, builder *artifacts.Builder, out *Ingested) (result error) {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	if info == nil {
		if info, err = file.Stat(); err != nil {
			return err
		}
	}
	put, err := services.CAS.Put(ctx, file)
	if err != nil {
		return fmt.Errorf("treeio: %s: %w", relative, err)
	}
	if err := builder.Add(artifacts.Entry{Path: relative, Content: put.ID, Size: put.Size, Mode: options.mode()(relative, info)}); err != nil {
		return fmt.Errorf("treeio: %s: %w", relative, err)
	}
	out.Files++
	out.Bytes += put.Size
	return nil
}

// MaterializeOptions controls writing a tree out. Overwrite replaces existing files;
// without it an existing file fails the operation.
type MaterializeOptions struct {
	Overwrite bool
}

// Materialized reports what was written.
type Materialized struct {
	Files, Dirs int64
	Bytes       int64
}

// Materialize writes an artifact's expanded tree under dest, creating it if needed.
// Every operation goes through an os.Root, so neither a stored path nor a symlink
// already in dest can land a write outside it. Each file is verified and then renamed
// into place, so a corrupt file never appears at its final path. A failure leaves
// earlier files in place: treat dest as incomplete and discard it.
func Materialize(ctx context.Context, store *artifacts.Service, id artifacts.ID, dest string, options MaterializeOptions) (out Materialized, result error) {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return out, err
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		return out, err
	}
	defer func() { result = errors.Join(result, root.Close()) }()
	cursor := ""
	for {
		page, err := store.ListFiles(ctx, id, artifacts.FileQuery{Expand: true, Limit: 1000, Cursor: cursor})
		if err != nil {
			return out, err
		}
		for _, entry := range page.Entries {
			if err := writeEntry(ctx, store, id, root, entry, options, &out); err != nil {
				return out, fmt.Errorf("treeio: %s: %w", entry.Path, err)
			}
		}
		if cursor = page.NextCursor; cursor == "" {
			return out, nil
		}
	}
}

func writeEntry(ctx context.Context, store *artifacts.Service, id artifacts.ID, root *os.Root, entry artifacts.Entry, options MaterializeOptions, out *Materialized) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	local := filepath.FromSlash(entry.Path)
	if !filepath.IsLocal(local) {
		return ErrEscape
	}
	if entry.Kind == artifacts.KindDir {
		if err := root.MkdirAll(local, 0o755); err != nil {
			return wrapEscape(err)
		}
		out.Dirs++
		return nil
	}
	if !entry.IsFile() {
		return fmt.Errorf("treeio: unexpanded entry kind %q", entry.Kind)
	}
	if dir := filepath.Dir(local); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return wrapEscape(err)
		}
	}
	if !options.Overwrite {
		if _, err := root.Lstat(local); err == nil {
			return fs.ErrExist
		} else if !errors.Is(err, fs.ErrNotExist) {
			return wrapEscape(err)
		}
	}
	mode := fs.FileMode(entry.Mode).Perm()
	if mode == 0 {
		mode = 0o644
	}
	temp := local + ".treeio-partial"
	written, err := writeVerified(ctx, store, id, root, entry.Path, temp)
	if err != nil {
		return errors.Join(err, removeIfPresent(root, temp))
	}
	if err := root.Chmod(temp, mode); err != nil {
		return errors.Join(err, removeIfPresent(root, temp))
	}
	if err := root.Rename(temp, local); err != nil {
		return errors.Join(wrapEscape(err), removeIfPresent(root, temp))
	}
	out.Files++
	out.Bytes += written
	return nil
}

func writeVerified(ctx context.Context, store *artifacts.Service, id artifacts.ID, root *os.Root, path, temp string) (written int64, result error) {
	reader, err := store.OpenFile(ctx, id, path)
	if err != nil {
		return 0, err
	}
	defer func() { result = errors.Join(result, reader.Close()) }()
	file, err := root.OpenFile(temp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, wrapEscape(err)
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	return io.Copy(file, reader)
}

func removeIfPresent(root *os.Root, path string) error {
	if err := root.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// wrapEscape gives os.Root's refusal to leave the tree this package's sentinel.
func wrapEscape(err error) error {
	if err != nil && strings.Contains(err.Error(), "escapes") {
		return errors.Join(ErrEscape, err)
	}
	return err
}
