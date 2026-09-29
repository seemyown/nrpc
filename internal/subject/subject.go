package subject

import (
	"fmt"
	"strings"
)

// Pattern is a compiled subject pattern.
type Pattern struct {
	Raw        string // e.g. user.:id
	NATS       string // e.g. user.*
	segments   []seg
	paramNames []string // ordered named params
}

type segKind int

const (
	segLiteral segKind = iota
	segParam           // :name → *
	segStar            // *
	segFull            // >
)

type seg struct {
	kind segKind
	lit  string // literal value or param name
}

// Compile turns a route pattern into a NATS subject and match metadata.
func Compile(pattern string) (*Pattern, error) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil, fmt.Errorf("empty subject pattern")
	}
	parts := strings.Split(pattern, ".")
	p := &Pattern{Raw: pattern}
	natsParts := make([]string, 0, len(parts))
	for i, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("empty segment in pattern %q", pattern)
		}
		switch {
		case part == ">":
			if i != len(parts)-1 {
				return nil, fmt.Errorf("'>' must be last segment in %q", pattern)
			}
			p.segments = append(p.segments, seg{kind: segFull})
			natsParts = append(natsParts, ">")
		case part == "*":
			p.segments = append(p.segments, seg{kind: segStar})
			natsParts = append(natsParts, "*")
		case strings.HasPrefix(part, ":"):
			name := part[1:]
			if name == "" {
				return nil, fmt.Errorf("empty param name in %q", pattern)
			}
			p.segments = append(p.segments, seg{kind: segParam, lit: name})
			p.paramNames = append(p.paramNames, name)
			natsParts = append(natsParts, "*")
		default:
			if strings.ContainsAny(part, "*>") {
				return nil, fmt.Errorf("invalid segment %q in %q", part, pattern)
			}
			p.segments = append(p.segments, seg{kind: segLiteral, lit: part})
			natsParts = append(natsParts, part)
		}
	}
	p.NATS = strings.Join(natsParts, ".")
	return p, nil
}

// Match checks whether subject matches the pattern and returns named params.
func (p *Pattern) Match(subject string) (map[string]string, bool) {
	if p == nil {
		return nil, false
	}
	parts := strings.Split(subject, ".")
	params := make(map[string]string)
	si := 0
	for i, s := range p.segments {
		switch s.kind {
		case segLiteral:
			if si >= len(parts) || parts[si] != s.lit {
				return nil, false
			}
			si++
		case segParam, segStar:
			if si >= len(parts) || parts[si] == "" {
				return nil, false
			}
			if s.kind == segParam {
				params[s.lit] = parts[si]
			}
			si++
		case segFull:
			if i != len(p.segments)-1 {
				return nil, false
			}
			// remainder may be empty only if subject ends exactly here — NATS > requires at least one token typically,
			// but we accept remaining tokens (including zero for leniency when subject equals prefix).
			if si > len(parts) {
				return nil, false
			}
			return params, true
		}
	}
	if si != len(parts) {
		return nil, false
	}
	return params, true
}
