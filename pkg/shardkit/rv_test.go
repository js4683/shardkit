package shardkit

import (
	"testing"
)

// TestMaxRV pins evidence folding: numeric versions keep the
// maximum; any unparseable version sticks so the barrier fails
// closed on that type.
func TestMaxRV(t *testing.T) {
	cases := []struct {
		name     string
		recorded string
		observed string
		want     string
	}{
		{"empty recorded takes observed", "", "42", "42"},
		{"greater observed wins", "42", "43", "43"},
		{"lesser observed loses", "43", "42", "43"},
		{"equal stays", "42", "42", "42"},
		{"unparseable observed sticks", "42", "abc", "abc"},
		{"unparseable recorded sticks", "abc", "42", "abc"},
		{"empty observed keeps recorded", "42", "", "42"},
	}
	for _, c := range cases {
		if got := maxRV(c.recorded, c.observed); got != c.want {
			t.Errorf("%s: maxRV(%q, %q) = %q, want %q",
				c.name, c.recorded, c.observed, got, c.want)
		}
	}
}

// TestReached pins barrier comparison: the collection must reach a
// numeric want; anything unparseable fails closed.
func TestReached(t *testing.T) {
	cases := []struct {
		name       string
		collection string
		want       string
		pass       bool
	}{
		{"equal passes", "42", "42", true},
		{"greater passes", "100", "42", true},
		{"lesser waits", "41", "42", false},
		{"unparseable want fails", "100", "abc", false},
		{"unparseable collection fails", "abc", "42", false},
		{"empty want fails", "100", "", false},
	}
	for _, c := range cases {
		if got := reached(c.collection, c.want); got != c.pass {
			t.Errorf("%s: reached(%q, %q) = %v, want %v",
				c.name, c.collection, c.want, got, c.pass)
		}
	}
}
