package shardkit

import (
	"strconv"
)

// ResourceVersion comparison for fencing evidence. kube-apiserver
// (etcd-backed) versions are decimal; anything else is unorderable
// and fails closed at the freshness barrier.

// parseRV parses a decimal resourceVersion.
func parseRV(rv string) (uint64, bool) {
	n, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// maxRV folds one more observed version into the recorded one:
// numeric versions keep the maximum (concurrent same-type writes may
// complete out of order); any unparseable version sticks, forcing
// the freshness barrier to fail closed on that type.
func maxRV(recorded, observed string) string {
	if recorded == "" {
		return observed
	}
	if observed == "" {
		return recorded
	}
	rn, rok := parseRV(recorded)
	on, ook := parseRV(observed)
	if !rok || !ook {
		if !ook {
			return observed
		}
		return recorded
	}
	if on > rn {
		return observed
	}
	return recorded
}

// reached reports whether collection reached (or passed) want. An
// unparseable want never passes: the barrier fails closed instead of
// guessing at extension-server versioning.
func reached(collection, want string) bool {
	cn, cok := parseRV(collection)
	wn, wok := parseRV(want)
	if !cok || !wok {
		return false
	}
	return cn >= wn
}
