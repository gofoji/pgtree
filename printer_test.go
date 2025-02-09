package pgtree_test

import (
	"fmt"
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
		{"basic", "select * from foo", "SELECT * FROM foo", ""},
		{"error", "CREATE PUBLICATION mypublication FOR TABLE users, departments;", "", "CreatePublicationStmt not implemented"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, _ := pgtree.Parse(test.sql)
			got, _, err := pgtree.Debug(root.Stmts[0].Stmt)
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

	_, err := pgtree.Print(root.Stmts[0].Stmt)
	if err == nil || wantError != err.Error() {
		t.Errorf("Err = %v, %v", err, wantError)
	}
	_, err = pgtree.PrettyPrint(root.Stmts[0].Stmt)
	if err == nil || wantError != err.Error() {
		t.Errorf("Err = %v, %v", err, wantError)
	}
}

func TestLower(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
		err  string
	}{
		{"basic", "SELEcT * FrOm foo", "select * from foo;\n", ""},
	}

	opts := pgtree.DefaultFormat
	opts.LowerKeyword = true

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, _ := pgtree.Parse(test.sql)
			got, err := pgtree.PrintWithOptions(root.Stmts[0].Stmt, opts)
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

func ExamplePrint() {
	sql := "select * from foo left join bar on foo.id = bar.id;"

	root, err := pgtree.Parse(sql)
	if err != nil {
		fmt.Println("Parse error: ", err)
		return
	}

	outSQL, err := pgtree.Print(root.Stmts[0].Stmt)
	if err != nil {
		fmt.Println("Print error: ", err)
		return
	}

	fmt.Println(outSQL)
	// Output: SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id;
}

func ExamplePrettyPrint() {
	sql := "select * from foo left join bar on foo.id = bar.id;"

	root, err := pgtree.Parse(sql)
	if err != nil {
		fmt.Println("Parse error: ", err)
		return
	}

	outSQL, err := pgtree.PrettyPrint(root.Stmts[0].Stmt)
	if err != nil {
		fmt.Println("PrettyPrint error: ", err)
		return
	}

	fmt.Println(outSQL)
	// Output: SELECT
	//     *
	// FROM
	//     foo
	//     LEFT JOIN bar ON foo.id = bar.id;
}

func ExampleDebug() {
	sql := "select * from foo;"

	root, err := pgtree.Parse(sql)
	if err != nil {
		fmt.Println("Parse error: ", err)
		return
	}

	outSQL, debug, err := pgtree.Debug(root.Stmts[0].Stmt)
	if err != nil {
		fmt.Println("Debug error: ", err)
		return
	}

	fmt.Println(outSQL)
	fmt.Println(debug)
	// Output: SELECT * FROM foo
	// [Node_SelectStmt = `SELECT * FROM foo`
	//      Node_RangeVar = `foo`
	//      Node_ResTarget = `*`
	//          Node_ColumnRef = `*`
	//              Node_AStar = `*`
	//      Node_RangeVar = `foo`
	//      Node_ResTarget = `*`
	//          Node_ColumnRef = `*`
	//              Node_AStar = `*`
	// ]
}

func ExamplePrintWithOptions() {
	sql := "select a::int from foo;"

	root, err := pgtree.Parse(sql)
	if err != nil {
		fmt.Println("Parse error: ", err)
		return
	}

	opts := pgtree.DefaultFormat
	opts.LowerKeyword = true
	opts.Padding = "  "
	opts.UpperType = true
	opts.SimpleLen = 0 // Forces all

	outSQL, err := pgtree.PrintWithOptions(root.Stmts[0].Stmt, opts)
	if err != nil {
		fmt.Println("PrintWithOptions error: ", err)
		return
	}

	fmt.Println(outSQL)
	// Output: select
	//   a::INT
	// from
	//   foo;
}

func ExampleDefaultFormat() {
	sql := "select a::int from foo;"

	root, err := pgtree.Parse(sql)
	if err != nil {
		fmt.Println("Parse error: ", err)
		return
	}

	opts := pgtree.DefaultFormat
	opts.Padding = "\t"
	opts.SimpleLen = 0 // Forces all

	outSQL, err := pgtree.PrintWithOptions(root.Stmts[0].Stmt, opts)
	if err != nil {
		fmt.Println("PrintWithOptions error: ", err)
		return
	}

	fmt.Println(outSQL)
	// Output: SELECT
	// 	a::int
	// FROM
	// 	foo;
}
