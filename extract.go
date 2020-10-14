package pgtree

import (
	"fmt"
	"strings"

	"github.com/gofoji/pgtree/nodes"
)

// ExtractString is a simple utility to join the output of all String nodes by the sep.
func ExtractString(list []nodes.Node, sep string) string {
	return strings.Join(extractStrings(list), sep)
}

func extractStrings(list []nodes.Node) []string {
	var result []string

	for _, n := range list {
		s, ok := n.(*nodes.String)
		if ok {
			result = append(result, s.Str)
		}
	}

	return result
}

// TableRef includes all information for describing tables discovered in a SQL statement.
type TableRef struct {
	Catalog string
	Schema  string
	Table   string
	Alias   string
	Ref     *nodes.RangeVar
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

// ExtractTables returns all tables identified in the SQL.
func ExtractTables(node nodes.Node) []TableRef {
	var result []TableRef

	Walk(node, nil, func(node nodes.Node, stack []nodes.Node, v Visitor) Visitor {
		switch n := node.(type) {
		case *nodes.RangeVar:
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

// QueryParam defines a param parsed from the SQL.
type QueryParam struct {
	Name      string
	Type      string
	Reference *nodes.ColumnRef
}

func (p QueryParam) String() string {
	name := p.Name

	if p.Type != "" {
		name += "::" + p.Type
	}

	if p.Reference != nil {
		return fmt.Sprintf("`%s = %s`", name, ExtractString(p.Reference.Fields, "."))
	}

	return name
}

func extractParamNameAndType(node *nodes.AExpr) (string, string) {
	switch n := node.Rexpr.(type) {
	case *nodes.ColumnRef:
		return ExtractString(n.Fields, "??"), ""
	case *nodes.TypeCast:
		t, err := Print(n.TypeName)
		if err != nil {
			return "", ""
		}

		name, err := Print(n.Arg)
		if err != nil {
			return "", ""
		}

		return name, t
	}

	return "", ""
}

func findReference(parent nodes.Node) *nodes.ColumnRef {
	p, ok := parent.(*nodes.AExpr)
	if ok {
		r, ok := p.Lexpr.(*nodes.ColumnRef)
		if ok {
			return r
		}

		r, ok = p.Rexpr.(*nodes.ColumnRef)
		if ok {
			return r
		}
	}

	return nil
}

const paramToken = "@"

// ExtractParams finds all unique named params in the SQL.
//
// Example Usage:
//
//    sql := "select * from foo where id = @myParam"
//    root, _ := pgtree.Parse(sql)
//    params := pgtree.ExtractParams(root)
//    fmt.Println(params)
//
// Output
//
//    [`myparam = id`]
//
func ExtractParams(node nodes.Node) Params {
	var result Params

	Walk(node, nil, func(node nodes.Node, stack []nodes.Node, v Visitor) Visitor {
		switch n := node.(type) {
		case *nodes.AExpr:
			if ExtractString(n.Name, "") == paramToken {
				name, t := extractParamNameAndType(n)
				if name != "" {
					p := QueryParam{
						Name:      name,
						Type:      t,
						Reference: findReference(stack[len(stack)-1]),
					}
					result = append(result, &p)
				}

				return nil
			}
		}

		return v
	})

	return result
}

// Params is an []*QueryParam, it is typed to provide a helper for looking up by name.
type Params []*QueryParam

// IndexOf returns the index of Param matching the name, otherwise -1 if not found.
func (pp Params) IndexOf(name string) int {
	for i, p := range pp {
		if p.Name == name {
			return i
		}
	}

	return -1
}
