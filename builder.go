package pgtree

import (
	"strings"
	"unicode"
)

type sqlBuilder struct {
	FormatOptions
	ss []string
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

func (c *sqlBuilder) append(ss ...string) {
	c.ss = append(c.ss, filterEmpty(ss)...)
}

func (c *sqlBuilder) appendPadded(s string) {
	if !c.Pretty {
		c.append(s)

		return
	}

	ss := strings.Split(s, "\n")
	for j := range ss {
		if len(ss[j]) == 0 {
			continue
		}

		c.ss = append(c.ss, strings.TrimRight(c.Padding+ss[j]+"\n", " "))
	}
}

func (c *sqlBuilder) addToLast(s string) {
	c.ss[len(c.ss)-1] = c.ss[len(c.ss)-1] + s
}

func (c *sqlBuilder) LF() {
	if c.Pretty {
		c.ss = append(c.ss, "\n")
	}
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func endsWithSpace(s string) bool {
	l := len(s)
	i := s[l-1]

	return asciiSpace[i] == 1
}

func startsWithSpace(s string) bool {
	return len(s) > 0 && asciiSpace[s[0]] == 1
}

func (c *sqlBuilder) join(sep string) string {
	lenSS := len(c.ss)
	switch lenSS {
	case 0:
		return ""
	case 1:
		return c.ss[0]
	}

	n := len(sep) * (lenSS - 1)

	for i := 0; i < lenSS; i++ {
		n += len(c.ss[i])
	}

	var b strings.Builder

	b.Grow(n)
	b.WriteString(c.ss[0])
	last := c.ss[0]

	for _, s := range c.ss[1:] {
		if !endsWithSpace(last) && !startsWithSpace(s) {
			b.WriteString(sep)
		}

		last = s
		b.WriteString(s)
	}

	return b.String()
}

func (c *sqlBuilder) lines() []string {
	return c.ss
}

func (c *sqlBuilder) keyword(s string) {
	c.append(c.FormatOptions.keyword(s))
}

func (c *sqlBuilder) keywordIf(s string, b bool) {
	if b {
		c.keyword(s)
	}
}

func (c *sqlBuilder) keywordIfElse(t, f string, b bool) {
	if b {
		c.keyword(t)
	} else {
		c.keyword(f)
	}
}

func hasUpper(s string) bool {
	for _, r := range s {
		if unicode.IsUpper(r) {
			return true
		}
	}

	return false
}

func requiresQuote(s string) bool {
	if strings.HasPrefix(s, `"`) {
		return false
	}

	if strings.Contains(s, "-") {
		return true
	}

	if strings.Contains(s, ".") {
		return true
	}

	return hasUpper(s)
}

func (c *sqlBuilder) identifier(s ...string) {
	var ss []string

	for _, n := range trims(s) {
		if n == "" {
			continue
		}

		if IsKeyword(n) || requiresQuote(n) {
			ss = append(ss, doubleQuote(n))
		} else {
			ss = append(ss, n)
		}
	}

	c.append(strings.Join(ss, "."))
}
