package pgtree

import "strings"

type sqlBuilder struct {
	opt FormatOptions
	ss  []string
}

func filterEmpty(ss []string) []string {
	result := make([]string, 0, len(ss))
	for _, s := range ss {
		s = strings.TrimRight(s, " ")
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

func (c *sqlBuilder) Append(ss ...string) {
	c.ss = append(c.ss, filterEmpty(ss)...)
}

func (c *sqlBuilder) LF() {
	c.ss = append(c.ss, "\n")
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func endsWithSpace(s string) bool {
	l := len(s)
	if l == 0 {
		return false
	}
	i := s[l-1]
	return asciiSpace[i] == 1
}

func (c sqlBuilder) Join(sep string) string {
	switch len(c.ss) {
	case 0:
		return ""
	case 1:
		return c.ss[0]
	}
	n := len(sep) * (len(c.ss) - 1)
	for i := 0; i < len(c.ss); i++ {
		n += len(c.ss[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(c.ss[0])
	last := c.ss[0]
	for _, s := range c.ss[1:] {
		if s == "" {
			continue
		}
		if !endsWithSpace(last) {
			b.WriteString(sep)
		}
		last = s
		b.WriteString(s)
	}
	return b.String()
}

func (c sqlBuilder) Lines() []string {
	return c.ss
}

func (c *sqlBuilder) keyword(s string) {
	if c.opt.LowerKeyword {
		c.Append(strings.ToLower(s))
	}
	c.Append(strings.ToUpper(s))
}
func (c *sqlBuilder) keywordIf(s string, b bool) {
	if b {
		c.keyword(s)
	}
}

func RequiresQuote(s string) bool {
	if strings.Contains(s, "-") {
		return true
	}
	return false
}

func (c *sqlBuilder) identifier(s ...string) {
	var ss []string
	for _, n := range trims(s) {
		if n == "" {
			continue
		}
		if IsKeyword(n) || RequiresQuote(n) {
			ss = append(ss, doubleQuote(n))
		} else {
			ss = append(ss, n)
		}
	}

	c.Append(strings.Join(ss, "."))
}
