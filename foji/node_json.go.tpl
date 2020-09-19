// Code generated by foji {{ version }}, template: {{ templateFile }}; DO NOT EDIT.

package pgtree

import (
"encoding/json"
"fmt"
"strings"
)

func UnmarshalNodeJSON(input []byte) (node Node, err error) {
if input == nil || string(input) == "null" {
return
}

if strings.HasPrefix(string(input), "[") {
var list Nodes
list, err = UnmarshalNodeArrayJSON(input)
if err != nil {
return
}
node = list
return
}

var nodeMap map[string]json.RawMessage

err = json.Unmarshal(input, &nodeMap)
if err != nil {
return
}

for nodeType, jsonText := range nodeMap {
switch nodeType {
case "Node":
return UnmarshalNodeJSON(jsonText)

{{- range .Messages }}
	{{ if not (eq .MessageName "Node") }}
		case "{{ .MessageName }}":
		var outNode {{ pascal .MessageName }}
		err = json.Unmarshal(jsonText, &outNode)
		if err != nil {
		return
		}
		node = &outNode
	{{- end }}
{{- end }}
default:
err = fmt.Errorf("Could not unmarshal node of type %s and content %s", nodeType, jsonText)
return
}
}

return
}

func UnmarshalNodePtrJSON(input []byte) (nodePtr *Node, err error) {
if input == nil {
return
}

node, err := UnmarshalNodeJSON(input)
if err != nil {
return
}

nodePtr = &node
return
}

func UnmarshalNodeArrayJSON(input []byte) (nodes Nodes, err error) {
var items []json.RawMessage

err = json.Unmarshal(input, &items)
if err != nil {
return
}

for _, itemJSON := range items {
var node Node
node, err = UnmarshalNodeJSON(itemJSON)
if err != nil {
return
}

nodes = append(nodes, node)
}

return
}

func UnmarshalNodeArrayArrayJSON(input []byte) (nodeLists []Nodes, err error) {
var items []json.RawMessage

err = json.Unmarshal(input, &items)
if err != nil {
return
}

for _, itemJSON := range items {
var nodeList Nodes
nodeList, err = UnmarshalNodeArrayJSON(itemJSON)
if err != nil {
return
}

nodeLists = append(nodeLists, nodeList)
}

return
}

{{- range .Messages }}
	{{ if not (eq .MessageName "Node") }}
		func (node *{{ pascal .MessageName }}) MarshalJSON() (result []byte, err error) {
		if node == nil {
		return nil, nil
		}
		fields := map[string]interface{}{}

		{{- range .Fields }}
			{{- if (eq ($.GetType . "pgtree") "Node")  }}
				if node.{{pascal .FieldName}} != nil {
				fields["{{ default .FieldName (.OptionByName "json_name")}}"] = node.{{pascal .FieldName}}
				}
			{{- else }}
				{{- if hasPrefix "*" ($.GetType . "pgtree") }}
					if node.{{pascal .FieldName}} != nil {
					fields["{{ default .FieldName (.OptionByName "json_name")}}"] =  node.{{pascal .FieldName}}
					}
				{{- else }}
					fields["{{ default .FieldName (.OptionByName "json_name")}}"] =  node.{{pascal .FieldName}}
				{{- end }}
			{{- end }}
		{{- end }}

		return json.Marshal(map[string]interface{}{
		"{{ .MessageName }}": fields,
		})
		}

		func (node *{{ pascal .MessageName }}) UnmarshalJSON(input []byte) (err error) {
		var fields map[string]json.RawMessage

		err = json.Unmarshal(input, &fields)
		if err != nil {
		return
		}
		{{- range .Fields }}
			{{ $jsonName := default .FieldName (.OptionByName "json_name")}}
			{{ if (eq ($.GetType . "pgtree") "Node")  }}
				if fields["{{ $jsonName }}"] != nil {
				{{- if .IsRepeated }}
					node.{{pascal .FieldName}}, err = UnmarshalNodeArrayJSON(fields["{{ $jsonName }}"])
				{{- else}}
					node.{{pascal .FieldName}}, err = UnmarshalNodeJSON(fields["{{ $jsonName }}"])
				{{- end}}
				if err != nil {
				return
				}
				}
			{{- else }}
				if fields["{{ $jsonName }}"] != nil {
				err = json.Unmarshal(fields["{{ $jsonName }}"], &node.{{pascal .FieldName}})
				if err != nil {
				return
				}
				}
			{{- end }}
		{{- end }}

		return
		}
	{{- end }}
{{- end }}


func (node *Root) UnmarshalJSON(input []byte) (err error) {
node.Node, err = UnmarshalNodeJSON(input)
return
}

func (node *Root)  MarshalJSON() (result []byte, err error) {
return json.Marshal(node.Node)
}

{{- range .Enums }}
	{{ $enumType := .EnumName }}

	func (e *{{$enumType}}) UnmarshalJSON(input []byte) (err error) {
	var i int32
	err = json.Unmarshal(input, &i)
	if err == nil {
	*e = {{$enumType}}(i)
	return nil
	}
	var s string
	err = json.Unmarshal(input, &s)
	if err != nil {
	return err
	}
	*e = New{{$enumType}}(s)

	return nil
	}

	func (e *{{$enumType}})  MarshalJSON() (result []byte, err error) {
	return json.Marshal(e.String())
	}

{{- end }}
