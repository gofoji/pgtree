// Code generated by foji {{ version }}, template: {{ templateFile }}; DO NOT EDIT.

package pgtree

import (
"fmt"
)

{{- range .Enums }}
    {{ $enumType := .EnumName }}
    type {{ $enumType }} int32

    const (
    {{- range .Fields }}
        {{ .Ident }} {{ $enumType }} = {{ .Number }}
    {{- end }}
    )

    func New{{ $enumType }}(name string) {{ $enumType }} {
    switch name {
    {{- range .Fields }}
        case "{{ .Ident }}":
        return {{ .Ident }}
    {{- end }}
    }
    return {{ $enumType }}(0)
    }

    func (e {{ $enumType }}) String() string {
    switch e {
    {{- range .Fields }}
        case {{ .Ident }}:
        return "{{ .Ident }}"
    {{- end }}
    }
    return fmt.Sprintf("{{ $enumType }}(%d)", e)
    }

{{- end }}

type Node interface {
node()
}

type Root struct {
Node Node
}

func (*Root) node() {}

type Nodes []Node

func (Nodes) node() {}

{{- range .Messages }}
    {{ if not (eq .MessageName "Node") }}
        type {{ pascal .MessageName }} struct {
        {{- range .Fields }}
            {{ pascal .FieldName }} {{ if .IsRepeated }}[]{{ end }}{{ $.GetType . "pgtree" }} `json:"{{ default .FieldName (.OptionByName "json_name")}}",omitempty`
        {{- end }}
        }

        func (*{{ pascal .MessageName }}) node() {}

    {{- end }}
{{- end }}
