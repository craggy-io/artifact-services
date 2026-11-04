// Package typedclaim is the module-private capability boundary for validated
// publication. It is not a sandbox against code running inside this module.
package typedclaim

type Claim struct {
	schema, data, validator, encoding string
	valid                             bool
}

func New(schema, data, validator, encoding string) Claim {
	return Claim{schema, data, validator, encoding, true}
}
func (c Claim) Fields() (schema, data, validator, encoding string, valid bool) {
	return c.schema, c.data, c.validator, c.encoding, c.valid && c.schema != "" && c.data != "" && c.validator != "" && c.encoding != ""
}
