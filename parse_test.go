package pgtree_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofoji/pgtree"
)

func TestParseAndPretty(t *testing.T) {
	ff, err := filepath.Glob("testdata/*.sql")
	if err != nil {
		panic(err)
	}

	for _, test := range ff {
		if strings.HasSuffix(test, "_want.sql") {
			continue
		}
		t.Run(test, func(t *testing.T) {
			b, err := ioutil.ReadFile(test)
			if err != nil {
				t.Errorf("ReadFile error = %v", err)
				return
			}

			want, err := ioutil.ReadFile(FileWithExt(test, "_want.sql"))
			if err != nil {
				t.Errorf("ReadFile error = %v", err)
				return
			}

			got, err := testParse(string(b))
			if err != nil {
				if got == "" {
					t.Errorf("Parse error = %v", err)
					return
				}
				t.Errorf("Error: %v\ngot:\n`%v`\nwant:\n`%v`", err, got, string(want))
				return
			}
			if got != string(want) {
				t.Errorf("Mismatch diff:\n`%v`", diff(got, string(want)))
				return
			}
		})
	}
}

func diff(got, want string) string {
	var result []string
	gg := strings.Split(got, "\n")
	ww := strings.Split(want, "\n")
	for i, g := range gg {
		if ww[i] != g {
			result = append(result, "<<<<<", "`"+g+"`", ">>>>>", "`"+ww[i]+"`", "=====")
		} else {
			result = append(result, g)
		}
	}
	return strings.Join(result, "\n")
}

func FileWithExt(path, ext string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + ext
}

func testParse(sql string) (string, error) {
	var s2 string
	var n2 pgtree.Node

	n, err := pgtree.Parse(sql)
	if err != nil {
		return "", err
	}

	s, err := pgtree.PrettyPrint(n)
	if err != nil {
		return "", fmt.Errorf("pretty:%w", err)
	}

	n2, err = pgtree.Parse(s)
	if err != nil {
		return s, fmt.Errorf("re-parse:%w", err)
	}

	s2, err = pgtree.PrettyPrint(n2)
	if err != nil {
		return s, fmt.Errorf("re-pretty:%w", err)
	}

	if s != s2 {
		return s, errors.New("re-parse mismatch")
	}
	return s, nil
}
