package logs

import (
	"strings"
	"testing"
	"time"
)

func TestNewLogIDStartsWithLocalTime(t *testing.T) {
	before := time.Now().Format("20060102150405")
	logID := NewLogID()
	after := time.Now().Format("20060102150405")

	if len(logID) != 24 {
		t.Fatalf("logid should be 24 chars, got %q", logID)
	}
	prefix := logID[:14]
	if prefix < before || prefix > after {
		t.Fatalf("logid prefix %q not between %q and %q", prefix, before, after)
	}
	if NewLogID() == logID {
		t.Fatal("two logids should differ")
	}
}

func TestIsValidLogID(t *testing.T) {
	cases := map[string]bool{
		"20261028142000a1b2c3d4e5": true,
		"upstream-id_01":           true,
		"":                         false,
		"has space":                false,
		"line\nbreak":              false,
		strings.Repeat("a", 65):    false,
		strings.Repeat("a", 64):    true,
		"中文":                       false,
	}
	for logID, want := range cases {
		if got := IsValidLogID(logID); got != want {
			t.Errorf("IsValidLogID(%q) = %v, want %v", logID, got, want)
		}
	}
}
