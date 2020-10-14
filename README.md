# pgtree [![Build Status](https://travis-ci.org/gofoji/pgtree.svg?branch=master)](https://travis-ci.org/gofoji/pgtree) [![codecov](https://codecov.io/gh/gofoji/pgtree/branch/master/graph/badge.svg)](https://codecov.io/gh/gofoji/pgtree) [![PkgGoDev](https://pkg.go.dev/badge/github.com/gofoji/pgtree)](https://pkg.go.dev/github.com/gofoji/pgtree) [![Report card](https://goreportcard.com/badge/github.com/gofoji/pgtree)](https://goreportcard.com/report/github.com/gofoji/pgtree)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/gofoji/pgtree/build)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/gofoji/pgtree.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/gofoji/pgtree/alerts/)

[![forthebadge](https://forthebadge.com/images/badges/made-with-go.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)

Builds on the excellent work of [libpq_query](https://github.com/lfittl/libpg_query) to use the postgres (v12) source code to parse the SQL.  

This package builds an AST designed to make walking and mutating easier. Several basic helper functions are also
included.

In addition, it was built with a focus on ease of adding features and not performance. The is no intention to optimize
for zero copy or minimum allocations. Do *NOT* use pgtree in a performance critical process.

## Why

pgtree is used by [foji](https://github.com/gofoji/foji) to:
1. Validate and format the SQL 
1. Convert developer friendly params into valid SQL parameter references ([example](#Param-Extract))
1. Convert wildcard return attributes to point in time data model attributes  ( `*` => list of fields )

## Installation

```shell script
go get github.com/gofoji/pgtree
```

This has a dependency on the postgres source code (compile with C) and can impact build times significantly.

## Usage

### Parsing SQL

```go
sql := "select * from foo left join bar on foo.id = bar.id;"
root, err := pgtree.Parse(sql)
if err != nil {
    return err
}
```
The return value is a `Node` which is used in all the following examples.

### Print SQL (concise)
```go
outSQL, err := pgtree.Print(root)
if err != nil {
    return err
}
println(outSQL)
```
Output
```sql
SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id;
```

### Print SQL (pretty)
```go
outSQL, err := pgtree.PrettyPrint(root)
if err != nil {
    return err
}
println(outSQL)
```
Output
```sql
SELECT 
    * 
FROM 
    foo 
    LEFT JOIN bar ON foo.id = bar.id;
```

### Tables Extract

Finds all reference tables in the statement, including sub queries and joins

```go
sql := "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id"
root, _ := pgtree.Parse(sql)
tables := pgtree.ExtractTables(root)
fmt.Println(tables)
```
Output
```
[`foo` `bar`]
```

### Param Extract

```go
sql := "select * from foo where id = @myParam"
root, _ := pgtree.Parse(sql)
params := pgtree.ExtractParams(root)
fmt.Println(params)
```
Output
```
[`myparam = id`]
```
The returned `QueryParam` object includes a reference to the referenced column (`id` in this example) if it is available.  This is useful for type inference. 

### Param Replace
Building on the above example you can automatically replace all the instances of the named parameters with the place holder syntax `$#`

```go
pgtree.ReplaceParams(&root, params)
outSQL, _ := pgtree.Print(root)
fmt.Println(outSQL)
```
Output
```sql
SELECT * FROM foo WHERE id = $1;
```

## Writing Visitors

Visiting the SQL AST requires a function to match the Visitor signature:
```go
type Visitor func(node Node, stack []Node, v Visitor) Visitor
```
The first parameter is the current `Node` being visited, the `stack` provides the stack up to the root, the last parameter is the Visitor func.  
The returned value of the `Walk` is the visitor, passing back the input continues the traversal as usual, returning `nil` will stop walking the current branch at that node.  This is used in the example below, as there is no reason to continue walking after finding a `RangeVar`.  You can also return a different visitor func to change the downstream processing in more complex scenarios.

####Example

```go
func ExtractTables(node Node) []TableRef {
	var result []TableRef

	Walk(node, nil, func(node Node, stack []Node, v Visitor) Visitor {
		switch n := node.(type) {
		case *RangeVar:
			t := TableRef{
				Catalog: n.Catalogname,
				Schema:  n.Schemaname,
				Table:   n.Relname,
				Ref:     n,
			}
			if n.Alias != nil {
				t.Alias = n.Alias.Aliasname
			}
			result = append(result, t)
			return nil
		}
		return v
	})

	return result
}
```


## Debugging the SQL

When writing custom visitors and mutations it can be helpful to print the walk tree.

```go
sql := "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id"
root, _ := pgtree.Parse(sql)
_, _ = pgtree.Debug(root)
```
Output
```
Root = `SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id; \n `
    Nodes = `SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id; \n `
        RawStmt = `SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id; \n `
            SelectStmt = `SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id`
                JoinExpr = `foo LEFT JOIN bar ON foo.id = bar.id`
                    AExpr = `foo.id = bar.id`
                        String = `=`
                        ColumnRef = `bar.id`
                            String = `id`
                            String = `bar`
                        ColumnRef = `foo.id`
                            String = `id`
                            String = `foo`
                    RangeVar = `bar`
                    RangeVar = `foo`
                ResTarget = `*`
                    ColumnRef = `*`
                        AStar = `*`
                JoinExpr = `foo  \n LEFT JOIN bar ON foo.id = bar.id`
                    AExpr = `foo.id = bar.id`
                        String = `=`
                        ColumnRef = `bar.id`
                            String = `id`
                            String = `bar`
                        ColumnRef = `foo.id`
                            String = `id`
                            String = `foo`
                    RangeVar = `bar`
                    RangeVar = `foo`
                ResTarget = `*`
                    ColumnRef = `*`
                        AStar = `*`
```

To view the json view of the SQL ast
```go
sql := "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.id"
s, _ := pgtree.JSON(sql)
println(s)
```

Output (pretty printed for readability)

```json
[
  {
    "RawStmt": {
      "stmt": {
        "SelectStmt": {
          "targetList": [
            {
              "ResTarget": {
                "val": {
                  "ColumnRef": {
                    "fields": [
                      {
                        "A_Star": {}
                      }
                    ],
                    "location": 7
                  }
                },
                "location": 7
              }
            }
          ],
          "fromClause": [
            {
              "JoinExpr": {
                "jointype": "JOIN_LEFT",
                "larg": {
                  "RangeVar": {
                    "relname": "foo",
                    "inh": true,
                    "relpersistence": "p",
                    "location": 14
                  }
                },
                "rarg": {
                  "RangeVar": {
                    "relname": "bar",
                    "inh": true,
                    "relpersistence": "p",
                    "location": 28
                  }
                },
                "quals": {
                  "A_Expr": {
                    "name": [
                      {
                        "String": {
                          "str": "="
                        }
                      }
                    ],
                    "lexpr": {
                      "ColumnRef": {
                        "fields": [
                          {
                            "String": {
                              "str": "foo"
                            }
                          },
                          {
                            "String": {
                              "str": "id"
                            }
                          }
                        ],
                        "location": 35
                      }
                    },
                    "rexpr": {
                      "ColumnRef": {
                        "fields": [
                          {
                            "String": {
                              "str": "bar"
                            }
                          },
                          {
                            "String": {
                              "str": "id"
                            }
                          }
                        ],
                        "location": 44
                      }
                    },
                    "location": 42
                  }
                }
              }
            }
          ]
        }
      }
    }
  }
]
```

### Notes

Currently this uses the V12 branch of libpg_query which seems to fail for most cases when using the ProtoBuf serializer, instead we use the JSON format as the output of the Postgres core library.

parse_tree.proto pulled from [libpg_query PR67](https://github.com/lfittl/libpg_query/pull/67)

# TODO

- [ ] Define additional formatting options
- [ ] Add verbose option to inject long names
- [ ] Add concise option to convert syntax shorthands. Example:

```sql
CREATE TABLE table_name AS 
TABLE table1;
-- Parser expands to:
CREATE TABLE table_name AS
SELECT * FROM table1;
```
- [ ] Godocs
- [ ] Move generated code to internal package