package pgtree

// ErrInvalidParam is returned if a the Node graph has a parameter that is not defined in the params.
const ErrInvalidParam = pgtreeError("invalid param")

// ReplaceParams automatically replaces all the instances of the named parameters with the place holder syntax `$#`.
//
//    sql := "select * from foo where id = @myParam"
//    root, _ := pgtree.Parse(sql)
//    params := pgtree.ExtractParams(root)
//    pgtree.ReplaceParams(&root, params)
//    outSQL, _ := pgtree.Print(root)
//    fmt.Println(outSQL)
//
// Output
//
//    SELECT * FROM foo WHERE id = $1;
//
func ReplaceParams(root *Node, params Params) (err error) {
	mutate(root, nil, func(node *Node, stack []*Node, v MutateFunc) MutateFunc {
		switch n := (*node).(type) {
		case *AExpr:
			if ExtractString(n.Name, "") == paramToken {
				name, _ := extractParamNameAndType(n)
				i := params.IndexOf(name)
				if i < 0 {
					err = ErrInvalidParam.Wrap(name)

					return nil
				}

				p := ParamRef{Number: int32(i + 1)}
				*node = &p

				return nil
			}
		}

		return v
	})

	return
}
