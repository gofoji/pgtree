package pgtree

import (
	nodes "github.com/pganalyze/pg_query_go/v6"
)

// ErrInvalidParam is returned if the Node graph has a parameter that is not defined in the input params.
const ErrInvalidParam = pgtreeError("invalid param")

// ReplaceParams automatically replaces all the instances of the named parameters with the place holder syntax `$#`.
//
//	sql := "select * from foo where id = @myParam"
//	root, _ := pgtree.Parse(sql)
//	params := pgtree.ExtractParams(root)
//	pgtree.ReplaceParams(&root, params)
//	outSQL, _ := pgtree.Print(root)
//	fmt.Println(outSQL)
//
// Output
//
//	SELECT * FROM foo WHERE id = $1;
func ReplaceParams(root *nodes.Node, params Params) (err error) {
	mutate(root, nil, func(node *nodes.Node, stack []*nodes.Node, v MutateFunc) MutateFunc {
		switch n := (node.Node).(type) {
		case *nodes.Node_AExpr:
			if ExtractString(n.AExpr.Name, "") == paramToken {
				name, _ := extractParamNameAndType(n.AExpr)
				i := params.IndexOf(name)
				if i < 0 {
					err = ErrInvalidParam.Wrap(name)

					return nil
				}

				p := nodes.ParamRef{Number: int32(i + 1)}
				*node = nodes.Node{Node: &nodes.Node_ParamRef{ParamRef: &p}}

				return nil
			}
		}

		return v
	})

	return
}
