package table

import "fmt"

type scope struct {
	min string
	max string
}

func newScope(min, max string) *scope {
	return &scope{min, max}
}

func fusion(scopes []*scope) *scope {
	if len(scopes) == 0 {
		return nil
	}
	ret := &scope{
		min: scopes[0].min,
		max: scopes[0].max,
	}
	for i := 1; i < len(scopes); i++ {
		ret.min = min(scopes[i].min, ret.min)
		ret.max = max(scopes[i].max, ret.max)
	}
	return ret
}

func hasOverlap(s1, s2 *scope) bool {
	noOverlap := s1.max < s2.min || s1.min > s2.max
	return !noOverlap
}

func (s *scope) String() string {
	return fmt.Sprintf("[%q, %q]", s.min, s.max)
}

func scopeEqual(s1, s2 *scope) bool {
	return s1.min == s2.min && s1.max == s2.max
}
