package pgtree

import (
	"encoding/json"

	"github.com/lfittl/pg_query_go/parser"
)

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

func JSON(sql string) (string, error) {
	return parser.ParseToJSON(sql)
}
