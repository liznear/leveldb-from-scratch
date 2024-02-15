package model

import "testing"

func TestScope_Fusion(t *testing.T) {
	tcs := []struct {
		name     string
		scopes   []*Scope
		expected *Scope
	}{
		{
			name:     "Empty",
			scopes:   nil,
			expected: nil,
		},
		{
			name: "OneScope",
			scopes: []*Scope{
				NewScope("a", "b"),
			},
			expected: NewScope("a", "b"),
		},
		{
			name: "HaveOverlap",
			scopes: []*Scope{
				NewScope("a", "c"),
				NewScope("b", "d"),
			},
			expected: NewScope("a", "d"),
		},
		{
			name: "NoOverlap",
			scopes: []*Scope{
				NewScope("a", "c"),
				NewScope("e", "g"),
			},
			expected: NewScope("a", "g"),
		},
		{
			name: "ManyScopes",
			scopes: []*Scope{
				NewScope("a", "f"),
				NewScope("h", "k"),
				NewScope("aa", "zz"),
			},
			expected: NewScope("a", "zz"),
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := Fusion(tc.scopes)
			if got == nil {
				if tc.expected != nil {
					t.Errorf("Got nil, but want %v", tc.expected)
				}
				return
			}
			if got.min != tc.expected.min {
				t.Errorf("Got min %q, want %q", got.min, tc.expected.min)
			}
			if got.max != tc.expected.max {
				t.Errorf("Got max %q, want %q", got.max, tc.expected.max)
			}
		})
	}
}

func TestScope_HasOverlap(t *testing.T) {
	tcs := []struct {
		name string
		s1   *Scope
		s2   *Scope
		want bool
	}{
		{
			name: "HaveOverlap",
			s1:   NewScope("a", "c"),
			s2:   NewScope("b", "d"),
			want: true,
		},
		{
			name: "NoOverlap",
			s1:   NewScope("a", "c"),
			s2:   NewScope("e", "g"),
			want: false,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := HasOverlap(tc.s1, tc.s2)
			if got != tc.want {
				t.Errorf("Got %v, want %v", got, tc.want)
			}
		})
	}
}
