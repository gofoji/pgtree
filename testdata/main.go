package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/gofoji/pgtree"
)

var ErrMismatch = errors.New("sql print mismatch")

// Helper utility for debugging printer issues.
// Prints the source, deparsed SQL (minimal), pretty printed, debug trace, JSON structure, compares
// re-parsed pretty print to initial pretty print.
func main() {
	const sql = `
CREATE GLOBAL TEMP TABLE table_name(a, b)
WITH (    TOAST.VACUUM_TRUNCATE=true,
    USER_CATALOG_TABLE)
ON COMMIT DROP
TABLESPACE tablespace_name
AS
SELECT *
  FROM
      foo
WITH DATA;
`

	err := testParse(sql)
	if err != nil {
		fmt.Println("**", err)
		os.Exit(1)
	}

	fmt.Println("== NO ISSUES IDENTIFIED")
}

func testParse(sql string) error {
	fmt.Println("SOURCE:  ****************")
	fmt.Println(sql)

	root, err := pgtree.Parse(sql)
	if err != nil {
		return fmt.Errorf("parser:%w", err)
	}

	fmt.Println("DEPARSED:  ****************")

	deparsed, err := pgtree.Print(root)
	if err != nil {
		return fmt.Errorf("print:%w", err)
	}

	fmt.Println(deparsed)

	deparsedRoot, err := pgtree.Parse(deparsed)
	if err != nil {
		return fmt.Errorf("parser(deparsed):%w", err)
	}

	pretty, err := pgtree.PrettyPrint(deparsedRoot)
	if err != nil {
		return fmt.Errorf("prettyPrint:%w", err)
	}

	fmt.Println("PRETTY:  ****************")
	fmt.Println(pretty)
	fmt.Println("TRACE:  ****************")

	pretty2, trace, _ := pgtree.Debug(root)

	fmt.Println(trace)
	fmt.Println("JSON: ****************")

	j, err := pgtree.JSON(sql)
	if err != nil {
		return fmt.Errorf("json:%w", err)
	}

	var jsonTempObject []interface{}

	err = json.Unmarshal([]byte(j), &jsonTempObject)
	if err != nil {
		return fmt.Errorf("unmarshall:%w", err)
	}

	b, err := json.MarshalIndent(getStatementToPrint(jsonTempObject), "", "    ")
	if err != nil {
		return fmt.Errorf("marshallIndent:%w", err)
	}

	fmt.Println(string(b))

	if pretty != pretty2 {
		return ErrMismatch
	}

	return nil
}

func getStatementToPrint(in []interface{}) interface{} {
	if len(in) != 1 {
		return in
	}

	var printObject interface{} = in[0]

	m, ok := printObject.(map[string]interface{})
	if !ok {
		return printObject
	}

	if m["RawStmt"] == nil {
		return m
	}

	printObject = m["RawStmt"]

	m, ok = printObject.(map[string]interface{})
	if !ok {
		return printObject
	}

	if m["stmt"] != nil {
		return m["stmt"]
	}

	return printObject
}
