package nrpc

import (
	"strings"
)

// JoinSubject joins subject segments with "." skipping empty parts.
func JoinSubject(parts ...string) string {
	var b strings.Builder
	first := true
	for _, p := range parts {
		p = strings.Trim(p, ".")
		if p == "" {
			continue
		}
		if !first {
			b.WriteByte('.')
		}
		b.WriteString(p)
		first = false
	}
	return b.String()
}
