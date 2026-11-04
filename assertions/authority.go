package assertions

// Authority issues writer credentials and is the only thing that can. It verifies
// nothing about who asks: holding it is the permission, so a trusted construction
// path should create it, give it to the service through Options.Authority, hand out
// the credentials its callers need, and keep the Authority itself to itself.
//
// This is a capability boundary, not a sandbox. In-process Go cannot defend against
// code that uses reflection or unsafe to read a credential, that holds the Authority,
// or that opens its own service over the root with an Authority of its own (the
// root's lease stops that only while this service is live), or that edits journal
// files. It does stop everything else, including code that holds the Service handle
// and any amount of ordinary API access, from writing a restricted kind.
type Authority struct {
	// A non-zero size keeps distinct Authority values at distinct addresses, which
	// is what a credential's identity check rests on.
	_ byte
}

// NewAuthority returns a new, independent authority.
func NewAuthority() *Authority { return &Authority{} }

// Writer is an opaque credential. The zero Writer means no credential, and a Writer
// from another Authority is refused everywhere, even on unrestricted kinds, because
// recording it would attribute the write to an identity nobody verified.
type Writer struct {
	identity  string
	authority *Authority
}

// Credential issues a Writer for an identity, whose meaning is the caller's.
func (a *Authority) Credential(identity string) Writer {
	return Writer{identity: identity, authority: a}
}
