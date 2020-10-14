package pgtree_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gofoji/pgtree"
	"github.com/gofoji/pgtree/nodes"
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
			w := string(want)
			if got != w {
				t.Errorf("Mismatch diff:\n`%v`", diff(got, w))
				t.Errorf("got:\n`%v`\nwant:\n`%v`", got, w)
				return
			}
		})
	}

	ff, _ = filepath.Glob("temp/sql/*.sql")
	for _, test := range ff {
		t.Run(test, func(t *testing.T) {
			b, err := ioutil.ReadFile(test)
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
				t.Errorf("Error: %v\ngot:\n`%v`", err, got)
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
		if i >= len(ww) {
			result = append(result, "<<<<<got"+strconv.Itoa(i), "`"+g+"`", ">>>>>", "=====")
		} else if ww[i] != g {
			result = append(result, "<<<<<got"+strconv.Itoa(i), "`"+g+"`", ">>>>>", "`"+ww[i]+"`", "=====")
		} else {
			result = append(result, g)
		}
	}
	if len(gg) < len(ww) {
		result = append(result, "<<<<<got", ">>>>>", "`"+strings.Join(ww[len(gg):], "\n")+"`", "=====")
	}
	return strings.Join(result, "\n")
}

func FileWithExt(path, ext string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + ext
}

func testParse(sql string) (string, error) {
	var prettyConcise string
	var conciseNode nodes.Node

	// We validate the parsing and printing by:
	// first Parse the input SQL
	// then Print it (concise)
	// then Parse the concise (this ensures the generated syntax is valid)
	// then Pretty Print
	// then compare to the Pretty Print of the original parse
	inputNode, err := pgtree.Parse(sql)
	if err != nil {
		return "", err
	}

	concise, err := pgtree.Print(inputNode)
	if err != nil {
		return "", fmt.Errorf("pretty:%w", err)
	}

	conciseNode, err = pgtree.Parse(concise)
	if err != nil {
		return "", fmt.Errorf("re-parse:%w", err)
	}

	prettyConcise, err = pgtree.PrettyPrint(conciseNode)
	if err != nil {
		return "", fmt.Errorf("re-pretty:%w", err)
	}
	prettyInput, err := pgtree.PrettyPrint(inputNode)
	if err != nil {
		return "", fmt.Errorf("re-pretty:%w", err)
	}

	if prettyInput != prettyConcise {
		return "", errors.New("re-parse mismatch")
	}
	return prettyInput, nil
}
