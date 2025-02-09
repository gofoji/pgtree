package pgtree

import (
	"fmt"
	"strings"

	nodes "github.com/pganalyze/pg_query_go/v6"
)

// ExtractString is a simple utility to join the output of all String nodes by the sep.
func ExtractString(list []*nodes.Node, sep string) string {
	return strings.Join(extractStrings(list), sep)
}

func extractStrings(list []*nodes.Node) []string {
	var result []string

	for _, n := range list {
		s, ok := (n.Node).(*nodes.Node_String_)
		if ok {
			result = append(result, s.String_.Sval)
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
func ExtractTables(node *nodes.Node) []TableRef {
	var result []TableRef

	Walk(node, nil, func(node *nodes.Node, stack []*nodes.Node, v Visitor) Visitor {
		switch n := node.Node.(type) {
		case *nodes.Node_RangeVar:
			t := TableRef{
				Catalog: n.RangeVar.Catalogname,
				Schema:  n.RangeVar.Schemaname,
				Table:   n.RangeVar.Relname,
				Ref:     n.RangeVar,
			}

			if n.RangeVar.Alias != nil {
				t.Alias = n.RangeVar.Alias.Aliasname
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

func extractParamNameAndType(node *nodes.A_Expr) (string, string) {
	switch n := node.Rexpr.Node.(type) {
	case *nodes.Node_ColumnRef:
		return ExtractString(n.ColumnRef.Fields, "??"), ""
	case *nodes.Node_TypeCast:
		t, err := PrintWithOptions(&nodes.Node{Node: &nodes.Node_TypeName{TypeName: n.TypeCast.TypeName}}, DefaultFragmentFormat)
		if err != nil {
			return "", ""
		}

		name, err := PrintWithOptions(n.TypeCast.Arg, DefaultFragmentFormat)
		if err != nil {
			return "", ""
		}

		return name, t
	}

	return "", ""
}

func findReference(parent *nodes.Node) *nodes.ColumnRef {
	p, ok := parent.Node.(*nodes.Node_AExpr)
	if ok {
		r, ok := p.AExpr.Lexpr.Node.(*nodes.Node_ColumnRef)
		if ok {
			return r.ColumnRef
		}

		r, ok = p.AExpr.Rexpr.Node.(*nodes.Node_ColumnRef)
		if ok {
			return r.ColumnRef
		}
	}

	return nil
}

const paramToken = "@"

// ExtractParams finds all unique named params in the SQL.
//
// Example Usage:
//
//	sql := "select * from foo where id = @myParam"
//	root, _ := pgtree.Parse(sql)
//	params := pgtree.ExtractParams(root)
//	fmt.Println(params)
//
// Output
//
//	[`myparam = id`]
func ExtractParams(node *nodes.Node) Params {
	var result Params

	Walk(node, nil, func(node *nodes.Node, stack []*nodes.Node, v Visitor) Visitor {
		switch n := node.Node.(type) {
		case *nodes.Node_AExpr:
			if ExtractString(n.AExpr.Name, "") == paramToken {
				name, t := extractParamNameAndType(n.AExpr)
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
