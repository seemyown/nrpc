package subject_test

import (
	"testing"

	"github.com/seemyown/nrpc/internal/subject"
)

func TestCompileNamedParam(t *testing.T) {
	p, err := subject.Compile("user.:id")
	if err != nil {
		t.Fatal(err)
	}
	if p.NATS != "user.*" {
		t.Fatalf("nats subject: got %q", p.NATS)
	}
	params, ok := p.Match("user.42")
	if !ok {
		t.Fatal("expected match")
	}
	if params["id"] != "42" {
		t.Fatalf("param id: got %q", params["id"])
	}
}

func TestCompileFullWildcard(t *testing.T) {
	p, err := subject.Compile("events.>")
	if err != nil {
		t.Fatal(err)
	}
	if p.NATS != "events.>" {
		t.Fatalf("nats subject: got %q", p.NATS)
	}
	if _, ok := p.Match("events.user.created"); !ok {
		t.Fatal("expected match")
	}
}

func TestNoMatch(t *testing.T) {
	p, err := subject.Compile("user.:id")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Match("users.42"); ok {
		t.Fatal("expected no match")
	}
}
