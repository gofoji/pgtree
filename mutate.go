package pgtree

import "errors"

var ErrInvalidParam = errors.New("invalid param")

func ReplaceParams(root *Node, params Params) (err error) {
	mutate(root, nil, func(node *Node, stack []*Node, v MutateFunc) MutateFunc {
		switch n := (*node).(type) {
		case *AExpr:
			if ExtractString(n.Name, "") == paramToken {
				name, _ := extractParamNameAndType(n)
				i := params.IndexOf(name)
				if i < 0 {
					err = ErrInvalidParam

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
