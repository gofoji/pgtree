package pgtree_test

import (
	"fmt"
	"testing"

	"github.com/gofoji/pgtree"
)

func TestExtractParams(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"basic", "select * from foo where id = @myParam", "[`myparam = id`]"},
		{"typed", "select * from foo where id = @myParam::int", "[`myparam::int = id`]"},
		{"left side", "select * from foo where @myParam = foo", "[`myparam = foo`]"},
		{"no reference", "select *, @myParam from foo", "[myparam]"},
		{"bad param", "select *, @ from foo", "[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, _ := pgtree.Parse(test.sql)
			params := pgtree.ExtractParams(root)
			got := fmt.Sprint(params)
			if got != test.want {
				t.Errorf("ExtractParams() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestExtractTables(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"basic", "select * from foo where id = @myParam", "[`foo`]"},
		{"scoped", "select * from my_catalog.my_schema.foo as my_alias where id = @myParam", "[`my_catalog.my_schema.foo my_alias`]"},
		{"nested", "select * from foo where id in (select id from bar)", "[`foo` `bar`]"},
		{"cte", "WITH table_b AS (SELECT id, name FROM table_x WHERE id > 100) DELETE FROM table_a", "[`table_a` `table_x`]"},
		{"none", "select now()", "[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, _ := pgtree.Parse(test.sql)
			params := pgtree.ExtractTables(root)
			got := fmt.Sprint(params)
			if got != test.want {
				t.Errorf("ExtractTables() = %v, want %v", got, test.want)
			}
		})
	}
}
