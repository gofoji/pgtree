package pgtree

import (
	"fmt"
	"strings"
)

type FormatOptions struct {
	pretty       bool
	Padding      string
	LowerKeyword bool
	UpperType    bool
	SimpleLen    int
}
type printer struct {
	FormatOptions
	debug       bool
	debugOutput string
	level       int
	errs        []error
}

type Errors struct {
	errs []error
}

func (p Errors) Error() string {
	var result []string
	for _, e := range p.errs {
		result = append(result, e.Error())
	}
	return strings.Join(result, "\n")
}

func Print(root Node) (string, error) {
	p := printer{}

	result := p.printNode(root)
	if len(p.errs) > 0 {
		return "", Errors{p.errs}
	}
	return result, nil
}

func PrettyPrint(root Node) (string, error) {
	opt := FormatOptions{
		pretty:       true,
		Padding:      "    ",
		LowerKeyword: false,
		SimpleLen:    50,
	}
	p := printer{FormatOptions: opt}

	result := p.printNode(root)
	if p.debug {
		ss := strings.Split(p.debugOutput, "\n")
		l := len(ss) - 1
		for i := range ss {
			fmt.Println(ss[l-i])
		}
	}
	if len(p.errs) > 0 {
		return "", Errors{p.errs}
	}
	return result, nil
}

func Debug(root Node) (string, error) {
	opt := FormatOptions{
		pretty:       true,
		Padding:      "    ",
		LowerKeyword: false,
		SimpleLen:    70,
	}
	p := printer{FormatOptions: opt, debug: true}

	result := p.printNode(root)
	if p.debug {
		ss := strings.Split(p.debugOutput, "\n")
		l := len(ss) - 1
		for i := range ss {
			fmt.Println(ss[l-i])
		}
	}
	if len(p.errs) > 0 {
		return "", Errors{p.errs}
	}
	return result, nil
}

func (p *printer) addError(err error) {
	p.errs = append(p.errs, err)
}

func (p *printer) indent(s string, i int) string {
	return p.pad(i) + s
}

func (p *printer) pad(i int) string {
	if i <= 0 {
		return ""
	}
	return strings.Repeat(p.Padding, i)
}

func (p *printer) padLines(s string, i int) string {
	ss := strings.Split(s, "\n")
	for j := range ss {
		ss[j] = p.indent(ss[j], i)
	}
	return strings.Join(ss, "\n")
}

func (p *printer) printNodes(list Nodes, sep string) string {
	result := sqlBuilder{}
	for i := range list {
		result.Append(p.printNode(list[i]))
	}

	return result.Join(sep)
}

func (p *printer) printArr(list Nodes) []string {
	result := sqlBuilder{}
	for i := range list {
		result.Append(p.printNode(list[i]))
	}

	return result.Lines()
}

func trims(ss []string) []string {
	r := make([]string, len(ss))
	for i, s := range ss {
		r[i] = strings.TrimSpace(s)
	}
	return r
}

func IsKeyword(s string) bool {
	_, ok := keywords[s]
	return ok
}

func (p *printer) identifier(names ...string) string {
	b := sqlBuilder{}
	b.identifier(names...)
	return b.Join(".")
}

func (p *printer) keyword(s string) string {
	if p.LowerKeyword {
		return strings.ToLower(s)
	}
	return strings.ToUpper(s)
}

func (p *printer) builder() sqlBuilder {
	return sqlBuilder{opt: p.FormatOptions}
}
