package pgtree

import (
	"encoding/json"

	"github.com/lfittl/pg_query_go/parser"
)

// Parse uses the postgres 12 parsing engine to create a Node graph for walking and mutation.
func Parse(sql string) (Node, error) {
	r, err := JSON(sql)
	if err != nil {
		return nil, err
	}

	result := &Root{}

	err = json.Unmarshal([]byte(r), result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// JSON uses the postgres 12 parsing engine to generate the json of the graph.
func JSON(sql string) (string, error) {
	return parser.ParseToJSON(sql)
}
