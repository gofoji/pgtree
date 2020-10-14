package pgtree

import (
	"strings"

	"github.com/gofoji/pgtree/nodes"
)

// FormatOptions controls the formatting of the printer.
type FormatOptions struct {
	pretty                 bool   // When enabled
	OneResultColumnPerLine bool   // Forces each result item of a select statement to a new line
	LowerKeyword           bool   // If true it forces all keywords to lowercase.  Default is to force all to uppercase
	UpperType              bool   // If true it forces all types to uppercase.  Default is to force all to lower
	SimpleLen              int    // Statements shorter than SimpleLen will automatically have pretty printing disabled
	Padding                string // Used for indentation when Pretty printing
}

const (
	defaultSimpleLen = 50
	defaultPadding   = "    "
)

type printer struct {
	FormatOptions
	debug       bool
	level       int
	debugOutput []string
	errs        []error
}

// Print renders the Node with minimal spacing.
func Print(root nodes.Node) (string, error) {
	p := printer{}
	result := p.printNode(root)

	if len(p.errs) > 0 {
		return "", printErrors{p.errs}
	}

	return result, nil
}

// PrettyPrint renders the Node with indented formatting.
func PrettyPrint(root nodes.Node) (string, error) {
	opt := FormatOptions{
		pretty:                 true,
		OneResultColumnPerLine: true,
		Padding:                defaultPadding,
		SimpleLen:              defaultSimpleLen,
	}
	p := printer{FormatOptions: opt}
	result := p.printNode(root)

	if len(p.errs) > 0 {
		return "", printErrors{p.errs}
	}

	return result, nil
}

// Debug renders the Node with indented formatting and render graph.
// the second param is an indented trace of the call graph with results.  Very useful for
// defining new formatting rules or adding support for new Nodes.
func Debug(root nodes.Node) (string, []string, error) {
	opt := FormatOptions{
		pretty:                 true,
		OneResultColumnPerLine: true,
		Padding:                defaultPadding,
		SimpleLen:              defaultSimpleLen,
	}
	p := printer{FormatOptions: opt, debug: true}
	result := p.printNode(root)

	if len(p.errs) > 0 {
		return result, p.debugOutput, printErrors{p.errs}
	}

	return result, p.debugOutput, nil
}

func (p *printer) addError(err error) {
	p.errs = append(p.errs, err)
}

func (p *printer) pad(i int) string {
	if i <= 0 {
		return ""
	}

	return strings.Repeat(p.Padding, i)
}

func (p *printer) padLines(s string) string {
	ss := strings.Split(s, "\n")
	for j := range ss {
		ss[j] = strings.TrimRight(p.pad(1)+ss[j], " ")
	}

	return strings.Join(ss, "\n")
}

func (p *printer) printNodes(list nodes.Nodes, sep string) string {
	b := p.builder()

	for i := range list {
		b.append(p.printNode(list[i]))
	}

	return b.join(sep)
}

func (p *printer) printArr(list nodes.Nodes) []string {
	b := p.builder()

	for i := range list {
		b.append(p.printNode(list[i]))
	}

	return b.lines()
}

func trims(ss []string) []string {
	r := make([]string, len(ss))
	for i, s := range ss {
		r[i] = strings.TrimSpace(s)
	}

	return r
}

func (p *printer) identifier(names ...string) string {
	b := p.builder()
	b.identifier(names...)

	return b.join(".")
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
