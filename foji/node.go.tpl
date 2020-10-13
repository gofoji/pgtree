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
        {{- if not (eq .Number "0") }}
        case "{{ .Ident }}":
        return {{ .Ident }}
        {{- end }}
    {{- end }}
    }

    return {{ $enumType }}(0)
}


var  {{ $enumType }}String = map[{{ $enumType }}]string{
    {{- range .Fields }}
        {{ .Ident }}: "{{ .Ident }}",
    {{- end }}
}

func (e {{ $enumType }}) String() string {
    return {{ $enumType }}String[e]
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
