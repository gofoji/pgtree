package pgtree

import (
	"strings"

	"github.com/gofoji/pgtree/nodes"
)

// FormatOptions controls the formatting of the SQL output.
type FormatOptions struct {
	Pretty                 bool   // When enabled injects line feeds and indentation(Padding)
	OneResultColumnPerLine bool   // Forces each result item of a select statement to a new line.  Default to true.
	LowerKeyword           bool   // If true it forces all keywords to lowercase.  Default is to force all to uppercase.
	UpperType              bool   // If true it forces all types to uppercase.  Default is to force all to lower.
	SimpleLen              int    // Statements shorter than SimpleLen will automatically have pretty printing disabled (default 50).
	Padding                string // Used for indentation when Pretty printing.  Default is four spaces.
}

var DefaultFormat = FormatOptions{
	Pretty:                 true,
	OneResultColumnPerLine: true,
	Padding:                "    ",
	SimpleLen:              50,
}

type printer struct {
	FormatOptions
	debug       bool
	level       int
	debugOutput []string
	errs        []error
}

// PrintWithOptions renders the Node with the supplied format options.
func PrintWithOptions(root nodes.Node, opts FormatOptions) (string, error) {
	p := printer{FormatOptions: opts}
	result := p.printNode(root)

	if len(p.errs) > 0 {
		return "", printErrors{p.errs}
	}

	return result, nil
}

// Print renders the Node with minimal spacing.
func Print(root nodes.Node) (string, error) {
	return PrintWithOptions(root, FormatOptions{})
}

// PrettyPrint renders the Node with indented formatting.
func PrettyPrint(root nodes.Node) (string, error) {
	return PrintWithOptions(root, DefaultFormat)
}

// Debug renders the Node with indented formatting and render graph.
// the second param is an indented trace of the call graph with results.  Very useful for
// defining new formatting rules or adding support for new Nodes.
func Debug(root nodes.Node) (string, []string, error) {
	p := printer{FormatOptions: DefaultFormat, debug: true}
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

func (p *printer) builder() sqlBuilder {
	return sqlBuilder{FormatOptions: p.FormatOptions}
}

func (o *FormatOptions) keyword(s string) string {
	if o.LowerKeyword {
		return strings.ToLower(s)
	}

	return strings.ToUpper(s)
}
