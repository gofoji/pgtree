package pgtree

import (
	"fmt"
	"strings"
)

func ExtractString(list []Node, sep string) string {
	return strings.Join(ExtractStrings(list), sep)
}

func ExtractStrings(list []Node) []string {
	var result []string
	for _, n := range list {
		s, ok := n.(*String)
		if ok {
			result = append(result, s.Str)
		}
	}
	return result
}

type TableRef struct {
	Catalog string
	Schema  string
	Table   string
	Alias   string
	Ref     *RangeVar
}

func (t TableRef) String() string {
	s := ""
	if t.Catalog != "" {
		s += t.Catalog + "."
	}
	if t.Schema != "" {
		s += t.Schema + "."
	}
	s += t.Table
	if t.Alias != "" {
		s += " " + t.Alias
	}
	return "`" + s + "`"
}

func getString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func ExtractTables(node Node) []TableRef {
	var result []TableRef

	Walk(node, nil, func(node Node, stack []Node, v Visitor) Visitor {
		switch n := node.(type) {
		case *RangeVar:
			t := TableRef{
				Catalog: n.Catalogname,
				Schema:  n.Schemaname,
				Table:   n.Relname,
				Ref:     n,
			}
			if n.Alias != nil {
				t.Alias = n.Alias.Aliasname
			}
			result = append(result, t)
			return nil
		}
		return v
	})

	return result
}

type QueryParam struct {
	Name      string
	Reference *ColumnRef
}

func (p QueryParam) String() string {
	if p.Reference != nil {
		return fmt.Sprintf("`%s = %s`", p.Name, ExtractString(p.Reference.Fields, "."))
	}
	return p.Name
}

func extractParamName(node *AExpr) string {
	switch n := node.Rexpr.(type) {
	case *ColumnRef:
		return ExtractString(n.Fields, "??")
	}
	return ""
}

func findReference(parent Node) *ColumnRef {
	p, ok := parent.(*AExpr)
	if ok {
		r, ok := p.Lexpr.(*ColumnRef)
		if ok {
			return r
		}
		r, ok = p.Rexpr.(*ColumnRef)
		if ok {
			return r
		}
	}
	return nil
}

const paramToken = "@"

func ExtractParams(node Node) Params {
	var result Params
	Walk(node, nil, func(node Node, stack []Node, v Visitor) Visitor {
		switch n := node.(type) {
		case *AExpr:
			if ExtractString(n.Name, "") == paramToken {
				p := QueryParam{
					Name:      extractParamName(n),
					Reference: findReference(stack[len(stack)-1]),
				}
				result = append(result, &p)
				return nil
			}
		}
		return v
	})

	return result
}

type Params []*QueryParam

func (pp Params) IndexOf(name string) int {
	for i, p := range pp {
		if p.Name == name {
			return i
		}
	}
	return -1
}
