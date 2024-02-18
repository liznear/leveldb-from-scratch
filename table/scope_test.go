package table

import "testing"

func TestScope_Fusion(t *testing.T) {
	tcs := []struct {
		name     string
		scopes   []*scope
		expected *scope
	}{
		{
			name:     "Empty",
			scopes:   nil,
			expected: nil,
		},
		{
			name: "OneScope",
			scopes: []*scope{
				newScope("a", "b"),
			},
			expected: newScope("a", "b"),
		},
		{
			name: "HaveOverlap",
			scopes: []*scope{
				newScope("a", "c"),
				newScope("b", "d"),
			},
			expected: newScope("a", "d"),
		},
		{
			name: "NoOverlap",
			scopes: []*scope{
				newScope("a", "c"),
				newScope("e", "g"),
			},
			expected: newScope("a", "g"),
		},
		{
			name: "ManyScopes",
			scopes: []*scope{
				newScope("a", "f"),
				newScope("h", "k"),
				newScope("aa", "zz"),
			},
			expected: newScope("a", "zz"),
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := fusion(tc.scopes)
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
		s1   *scope
		s2   *scope
		want bool
	}{
		{
			name: "HaveOverlap",
			s1:   newScope("Key0", "Key4"),
			s2:   newScope("Key2", "Key3"),
			want: true,
		},
		{
			name: "NoOverlap",
			s1:   newScope("Key0", "Key1"),
			s2:   newScope("Key2", "Key3"),
			want: false,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := hasOverlap(tc.s1, tc.s2)
			if got != tc.want {
				t.Errorf("Got %v, want %v", got, tc.want)
			}
		})
	}
}
