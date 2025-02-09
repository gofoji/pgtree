package pgtree

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Parse uses the postgres 12 parsing engine to create a Node graph for walking and mutation.
func Parse(sql string) (*pg_query.ParseResult, error) {
	return pg_query.Parse(sql)
}

func PrintParseResult(parseResult *pg_query.ParseResult) (string, error) {
	out := ""
	for _, stmt := range parseResult.Stmts {
		p, err := Print(stmt.Stmt)
		if err != nil {
			s, ss, err := Debug(parseResult.Stmts[0].Stmt)
			fmt.Printf("Debug:\n%s\nGraph:\n%v\nError:%v\n", s, ss, err)
			return "", fmt.Errorf("print:%w", err)
		}
		out += p
	}

	return out, nil
}

func PrettyPrintParseResult(parseResult *pg_query.ParseResult) (string, error) {
	out := ""
	for _, stmt := range parseResult.Stmts {
		p, err := PrettyPrint(stmt.Stmt)
		if err != nil {
			return "", fmt.Errorf("print:%w", err)
		}
		out += p
	}

	return out, nil
}
