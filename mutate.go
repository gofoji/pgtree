package pgtree

func ReplaceParams(root *Node, params Params, paramToken string) {
	mutate(root, nil, func(node *Node, stack []*Node, v MutateFunc) MutateFunc {
		switch n := (*node).(type) {
		case *AExpr:
			if ExtractString(n.Name, "") == paramToken {
				name := extractParamName(n)
				i := params.IndexOf(name)
				if i < 0 {
					return nil
				}
				p := ParamRef{Number: int32(i + 1)}
				*node = &p
				return nil
			}
		}
		return v
	})
}
