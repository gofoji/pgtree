package pgtree_test

import (
	"testing"

	"github.com/gofoji/pgtree"
)

func TestDebug(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
		err  string
	}{
		{"basic", "select * from foo", "SELECT * FROM foo;\n", ""},
		{"error", "CREATE PUBLICATION mypublication FOR TABLE users, departments;", "", "CreatePublicationStmt not implemented"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, _ := pgtree.Parse(test.sql)
			got, err := pgtree.Debug(root)
			if err != nil && test.err != err.Error() {
				t.Errorf("Err = %v, want %v", err, test.err)
				return
			}
			if got != test.want {
				t.Errorf("got `%v`, want `%v`", got, test.want)
			}
		})
	}
}
