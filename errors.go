package pgtree

import (
	"fmt"
	"strings"
)

type pgtreeError string

func (err pgtreeError) Error() string {
	return string(err)
}

func (err pgtreeError) Wrap(s string) error {
	return fmt.Errorf("%s:%w", s, err)
}

const (
	// ErrPrinter is the base error for any printer errors (generally unimplemented features).
	ErrPrinter = pgtreeError("printer")
)

type printErrors struct {
	errs []error
}

func (p printErrors) Error() string {
	result := make([]string, len(p.errs))
	for i, e := range p.errs {
		result[i] = e.Error()
	}

	return strings.Join(result, "\n")
}
