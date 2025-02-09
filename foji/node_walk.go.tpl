// Code generated by foji {{ version }}, template: {{ templateFile }}; DO NOT EDIT.

package pgtree

import (
	nodes "github.com/pganalyze/pg_query_go/v6"
)

type Visitor func(node *nodes.Node, stack []*nodes.Node, v Visitor) Visitor

func WalkList(list []*nodes.Node, stack []*nodes.Node, v Visitor) {
	for _, n := range list {
		Walk(n, stack, v)
	}
}

func Walk(node *nodes.Node, stack []*nodes.Node, v Visitor) {
	if node == nil || isNilValue(node) {
		return
	}

	if v = v(node, stack, v); v == nil {
		return
	}

	stack = append(stack, node)
	switch n := node.Node.(type) {
{{- range .Messages }}
	{{- if and (not (in .MessageName "Node" "ParseResult" "ScanResult")) ($.HasMessage .) }}
		{{ $node := pascal .MessageName }}
		case *nodes.Node_{{ $node }}:
		{{- range .Fields }}
			{{- if $.IsMessage .Type }}
                {{ $fieldName := pascal .FieldName -}}
                {{ if eq $fieldName "String" -}}
                    {{ $fieldName = "String_" -}}
                {{ else if eq $fieldName "SQLBody" -}}
                    {{ $fieldName = "SqlBody" -}}
                {{ end -}}
				{{- if .IsRepeated }}
					WalkList(n.{{ $node }}.{{ pascal .FieldName }}, stack, v)
				{{- else if eq "Node" .Type -}}
					Walk(n.{{ $node }}.{{ $fieldName }}, stack, v)
				{{- else -}}
					{{ $type := .Type -}}
					{{ if eq $type "CTESearchClause" -}}
						{{ $type = "CtesearchClause" }}
					{{ else if eq $type "CTECycleClause"}}
                        {{ $type = "CtecycleClause" }}
					{{ else if eq $type "String"}}
                        {{ $type = "String_" }}
					{{- end}}
					if n.{{ $node }}.{{$fieldName}} != nil {
						Walk(&nodes.Node{Node:&nodes.Node_{{ $type }}{ {{ $type }}: n.{{ $node }}.{{$fieldName}} } }, stack, v)
					}
                {{- end }}
			{{ end }}
		{{- end }}
	{{- end }}
{{- end }}
{{/*	case *nodes.Root:*/}}
{{/*		Walk(n.Node, stack, v)*/}}
{{/*	case nodes.Nodes:*/}}
{{/*		WalkList(n, stack, v)*/}}
	}
}

type MutateFunc func(node *nodes.Node, stack []*nodes.Node, visitor MutateFunc) MutateFunc

func mutateList(list []*nodes.Node, stack []*nodes.Node, v MutateFunc) {
	for i := range list {
		mutate(list[i], stack, v)
	}
}

func mutate(node *nodes.Node, stack []*nodes.Node, v MutateFunc) {
{{/*	var nodeWrapper nodes.Node*/}}
	if node == nil || isNilValue(*node) {
		return
	}

	if v = v(node, stack, v); v == nil {
		return
	}

	stack = append(stack, node)

	switch n := (node.Node).(type) {
{{- range .Messages }}
	{{- if not (in .MessageName "Node" "ParseResult" "ScanResult" "ScanToken") }}
        {{ $message :=  pascal .MessageName }}
        {{ if eq $message "String" -}}
            {{ $message = "String_" -}}
        {{ end -}}
		case *nodes.Node_{{ $message }}:
		{{- range .Fields }}
			{{- if $.IsMessage .Type }}
				{{- if .IsRepeated }}
					mutateList(n.{{ $message }}.{{ pascal .FieldName }}, stack, v)
				{{- else }}
					{{- if eq .Type "Node" }}
                        {{ $fieldName := pascal .FieldName -}}
                        {{ if eq $fieldName "String" -}}
                            {{ $fieldName = "String_" -}}
                        {{ else if eq $fieldName "SQLBody" -}}
                            {{ $fieldName = "SqlBody" -}}
                        {{ end -}}

						mutate(n.{{ $message }}.{{ $fieldName }}, stack, v)  // {{ .Type }}
					{{- else }}
{{/*						nodeWrapper =  n.{{ $message }}.{{ pascal .FieldName }}*/}}
{{/*						mutate(nodeWrapper, stack, v)  // {{ .Type }}*/}}
					{{- end }}
				{{- end }}
			{{- end }}
		{{- end }}
	{{- end }}
{{- end }}
{{/*	case *nodes.Root:*/}}
{{/*		mutate(&n.Node, stack, v)*/}}
{{/*	case nodes.Nodes:*/}}
{{/*		mutateList(n, stack, v)*/}}
	}
}
