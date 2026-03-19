package myipsvc

import "testing"

func TestSplitProxyValues(t *testing.T) {
	values := splitProxyValues(" http://a:1 ; http://b:2,\nhttp://a:1 ")
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if values[0] != "http://a:1" || values[1] != "http://b:2" {
		t.Fatalf("unexpected values: %#v", values)
	}
}
