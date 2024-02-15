package model

type Scope struct {
	min string
	max string
}

func NewScope(min, max string) *Scope {
	return &Scope{min, max}
}

func Fusion(scopes []*Scope) *Scope {
	if len(scopes) == 0 {
		return nil
	}
	ret := &Scope{
		min: scopes[0].min,
		max: scopes[0].max,
	}
	for i := 1; i < len(scopes); i++ {
		ret.min = min(scopes[i].min, ret.min)
		ret.max = max(scopes[i].max, ret.max)
	}
	return ret
}

func HasOverlap(s1, s2 *Scope) bool {
	noOverlap := s1.max < s2.min || s1.min > s2.max
	return !noOverlap
}

func (s *Scope) String() string {
	return s.min + " - " + s.max
}
