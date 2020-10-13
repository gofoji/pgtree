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
			got, _, err := pgtree.Debug(root)
			if err != nil {
				if test.err != err.Error() {
					t.Errorf("Err = %v, want %v", err, test.err)
				}
				return
			}
			if got != test.want {
				t.Errorf("got `%v`, want `%v`", got, test.want)
			}
		})
	}
}

func TestErrors(t *testing.T) {
	const wantError = "CreatePublicationStmt not implemented"
	root, _ := pgtree.Parse("CREATE PUBLICATION mypublication FOR TABLE users, departments;")
	_, err := pgtree.Print(root)
	if err == nil || wantError != err.Error() {
		t.Errorf("Err = %v, %v", err, wantError)
	}
	_, err = pgtree.PrettyPrint(root)
	if err == nil || wantError != err.Error() {
		t.Errorf("Err = %v, %v", err, wantError)
	}

}
