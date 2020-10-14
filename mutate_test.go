package pgtree_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gofoji/pgtree"
)

func TestReplaceParams(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"basic", "select * from foo where id = @myParam", "SELECT * FROM foo WHERE id = $1;"},
		{"left side", "select * from foo where @myParam=id", "SELECT * FROM foo WHERE $1 = id;"},
		{"multiple", "select * from foo where id in (@myParam,@param2)", "SELECT * FROM foo WHERE id IN ($1, $2);"},
		{"no reference", "select *, @param from foo", "SELECT *, $1 FROM foo;"},
		{"typed", "select * from foo where id = @myParam::int", "SELECT * FROM foo WHERE id = $1;"},
		{"none", "select 1", "SELECT 1;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, _ := pgtree.Parse(test.sql)
			params := pgtree.ExtractParams(root)
			err := pgtree.ReplaceParams(&root, params)
			if err != nil {
				t.Errorf("Get Error = `%v`", err)
				return
			}
			got, _ := pgtree.Print(root)
			if got != test.want {
				t.Errorf("ReplaceParams() = %v, want %v", got, test.want)
			}
		})
	}

	t.Run("invalid map", func(t *testing.T) {
		root, _ := pgtree.Parse(tests[0].sql)
		err := pgtree.ReplaceParams(&root, pgtree.Params{})
		if !errors.Is(err, pgtree.ErrInvalidParam) {
			t.Errorf("Invalid Error, got `%v`, want `%v`", err, pgtree.ErrInvalidParam)
		}
	})
}

func ExampleReplaceParams() {
	sql := "select * from foo where id = @myParam"
	root, _ := pgtree.Parse(sql)
	params := pgtree.ExtractParams(root)
	pgtree.ReplaceParams(&root, params)
	outSQL, _ := pgtree.Print(root)
	fmt.Println(outSQL)
	// Output: SELECT * FROM foo WHERE id = $1;
}
