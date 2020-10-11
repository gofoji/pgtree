package pgtree

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func (p *printer) printJoinExpr(node *JoinExpr) string {
	result := sqlBuilder{}
	result.Append(p.printNode(node.Larg))
	if p.pretty {
		result.LF()
	}
	switch node.Jointype {
	case JOIN_INNER:
		if node.IsNatural {
			result.Append("NATURAL")
		} else if node.Quals == nil && len(node.UsingClause) == 0 {
			result.Append("CROSS")
		}
	case JOIN_LEFT:
		result.Append("LEFT")
	case JOIN_FULL:
		result.Append("FULL")
	case JOIN_RIGHT:
		result.Append("RIGHT")
	}
	result.Append("JOIN")
	result.Append(p.printNode(node.Rarg))
	if node.Quals != nil {
		result.Append("ON")
		result.Append(p.printNode(node.Quals))
	}

	if len(node.UsingClause) > 0 {
		columns := p.printNodes(node.UsingClause, ", ")
		result.Append(fmt.Sprintf("USING (%s)", columns))
	}

	return result.Join(" ")
}

func (p *printer) printResTarget(node *ResTarget) string {
	if node.Name != "" {
		v := p.printNode(node.Val)
		if v != "" {
			return fmt.Sprintf("%s AS %s", v, p.identifier(node.Name))
		}
		return p.identifier(node.Name)
	}
	return p.printNode(node.Val)
}

func (p *printer) printColumnRef(node *ColumnRef) string {
	b := p.builder()
	b.identifier(p.printArr(node.Fields)...)
	return b.Join(" ")
}

func (p *printer) printWithClause(node *WithClause) string {
	if node == nil {
		return ""
	}
	result := []string{"WITH"}
	if node.Recursive {
		result = append(result, "RECURSIVE")
	}
	subs := p.printNodes(node.Ctes, ", ")
	result = append(result, subs)
	return strings.Join(result, " ")

}

func (p *printer) printSelectStmt(node *SelectStmt) string {
	if !p.pretty {
		return p.printSelectStmtInternal(node)
	}
	r := p.printSelectStmtInternal(node)
	if len(r) > p.SimpleLen {
		return r
	}
	p.pretty = false
	r = p.printSelectStmtInternal(node)
	p.pretty = true
	return r
}
func (p *printer) printSelectStmtInternal(node *SelectStmt) string {
	b := p.builder()
	sub := p.printWithClause(node.WithClause)
	b.Append(sub)

	if node.Op != SETOP_NONE {
		b.Append(p.printNode(node.Larg))
		if p.pretty {
			b.LF()
		}

		switch node.Op {
		case SETOP_UNION:
			b.keyword("UNION")
		case SETOP_INTERSECT:
			b.keyword("INTERSECT")
		case SETOP_EXCEPT:
			b.keyword("EXCEPT")
		}
		b.keywordIf("ALL", node.All)
		if p.pretty {
			b.LF()
		}
		b.Append(p.printNode(node.Rarg))
	}
	if len(node.FromClause) > 0 || len(node.TargetList) > 0 {
		if p.pretty && sub != "" {
			b.LF()
		}
		b.keyword("SELECT")
		if p.pretty {
			b.LF()
		}
	}

	if len(node.TargetList) > 0 {
		if len(node.DistinctClause) > 0 {
			b.keyword("DISTINCT ON")
			b.Append(p.printSubClause(node.DistinctClause))
		}

		columns := p.printArr(node.TargetList)
		if p.pretty && p.OneResultColumnPerLine {
			columnsStr := strings.Join(columns, ",\n")
			b.Append(p.padLines(columnsStr, 1))
		} else {
			b.Append(strings.Join(columns, ", "))
		}

		if node.IntoClause != nil {
			b.keyword("INTO")
			b.Append(p.printNode(node.IntoClause))
		}
	}

	if len(node.FromClause) > 0 {
		if p.pretty {
			b.LF()
		}
		b.keyword("FROM")
		columns := p.printNodes(node.FromClause, ", ")

		if p.pretty {
			b.LF()
			columns = p.padLines(columns, 1)
		}
		b.Append(columns)
	}

	if node.WhereClause != nil {
		if p.pretty {
			b.LF()
		}
		b.keyword("WHERE")
		sub := p.printNode(node.WhereClause)
		if p.pretty {
			b.LF()
			sub = p.padLines(sub, 1)
		}
		b.Append(sub)
	}

	if len(node.ValuesLists) > 0 {
		if p.pretty {
			b.LF()
		}
		b.keyword("VALUES")
		if p.pretty {
			b.LF()
		}
		var vv []string
		for _, nl := range node.ValuesLists {
			vv = append(vv, fmt.Sprintf("(%s)", p.printNode(nl)))
		}
		if p.pretty {
			b.Append(p.padLines(strings.Join(vv, "\n"), 1))
		} else {
			b.Append(strings.Join(vv, " "))
		}
	}
	if len(node.GroupClause) > 0 {
		if p.pretty {
			b.LF()
		}
		b.keyword("GROUP BY")
		b.Append(p.printNodes(node.GroupClause, ", "))
	}
	if node.HavingClause != nil {
		if p.pretty {
			b.LF()
		}
		b.keyword("HAVING")
		b.Append(p.printNode(node.HavingClause))
	}
	if len(node.SortClause) > 0 {
		if p.pretty {
			b.LF()
		}
		b.keyword("ORDER BY")
		b.Append(p.printNodes(node.SortClause, ", "))
	}
	if node.LimitCount != nil {
		if p.pretty {
			b.LF()
		}
		b.keyword("LIMIT")
		b.Append(p.printNode(node.LimitCount))
	}
	if node.LimitOffset != nil {
		if p.pretty {
			b.LF()
		}
		b.keyword("OFFSET")
		b.Append(p.printNode(node.LimitOffset))
	}
	if len(node.LockingClause) > 0 {
		b.Append(p.printNodes(node.LockingClause, " "))
	}

	return b.Join(" ")
}

func (p *printer) printAExpr(node *AExpr) string {
	left := p.printNode(node.Lexpr)
	right := p.printNode(node.Rexpr)
	op := p.printNodes(node.Name, " ")

	switch node.Kind {
	case AEXPR_OP:
		return fmt.Sprintf("%s %s %s", left, op, right)
	case AEXPR_OP_ANY:
		return fmt.Sprintf("%s %s ANY(%s)", left, op, right)
	case AEXPR_IN:
		if op == "=" {
			op = "IN"
		} else {
			op = "NOT IN"
		}
		return fmt.Sprintf("%s %s (%s)", left, op, right)
	case AEXPR_LIKE:
		if op == "~~" {
			op = "LIKE"
		} else {
			op = "NOT LIKE"
		}
		return fmt.Sprintf("%s %s %s", left, op, right)
	}
	p.addError(errors.New("unhandled A_Expr kind type: " + node.Kind.String()))
	return ""
}

func (p *printer) printRangeVar(node *RangeVar) string {
	return p.printRangeVarInternal(node, false)
}

func (p *printer) printRangeVarInternal(node *RangeVar, ignoreInh bool) string {
	var result []string

	if !node.Inh && !ignoreInh {
		result = append(result, "ONLY")
	}
	schema := ""
	if node.Schemaname != "" {
		schema = p.identifier(node.Schemaname) + "."
	}
	result = append(result, schema+p.identifier(node.Relname))

	if node.Alias != nil {
		result = append(result, p.printAlias(node.Alias))
	}
	return strings.Join(result, " ")
}

func (p *printer) printAlias(node *Alias) string {
	if node == nil {
		return ""
	}

	if len(node.Colnames) > 0 {
		columns := p.printNodes(node.Colnames, ", ")
		return fmt.Sprintf("%s(%s)", node.Aliasname, columns)
	}
	return p.identifier(node.Aliasname)
}

func (p *printer) printParamRef(node *ParamRef) string {
	if node.Number == 0 {
		return "?"
	}
	return fmt.Sprintf("$%d", node.Number)
}

func (p *printer) printString(node *String) string {
	return p.identifier(node.Str)
}

func (p *printer) printAStar(_ *AStar) string {
	return "*"
}

func (p *printer) printRawStmt(node *RawStmt) string {
	term := ";"
	if p.pretty {
		term = ";\n"
	}
	return p.printNode(node.Stmt) + term
}

func (p *printer) printAConst(node *AConst) string {
	s, ok := node.Val.(*String)
	if ok {
		return quote(s.Str)
	}
	return p.printNode(node.Val)
}

func (p *printer) printInteger(node *Integer) string {
	return strconv.Itoa(int(node.Ival))
}

func (p *printer) printFloat(node *Float) string {
	return node.Str
}

func (p *printer) printBitString(node *BitString) string {
	return "B'" + node.Str[1:] + "'"
}

func (p *printer) printNull(_ *Null) string {
	return p.keyword("NULL")
}

func (p *printer) relPersistence(n *RangeVar) string {
	switch n.Relpersistence {
	case "t":
		return p.keyword("TEMPORARY")
	case "u":
		return p.keyword("UNLOGGED")
	}
	return ""
}

func (p *printer) printCreateStmt(node *CreateStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.Append(p.relPersistence(node.Relation))
	b.keyword("TABLE")

	if node.IfNotExists {
		b.keyword("IF NOT EXISTS")
	}
	b.Append(p.printNode(node.Relation))

	if node.OfTypename != nil {
		b.keyword("OF")
		b.identifier(p.printTypeName(node.OfTypename))
	}

	sub := p.printSubClause(node.TableElts)
	if sub == "" {
		// Empty table definitions are valid
		sub = "()"
	}
	b.Append(sub)

	if len(node.InhRelations) > 0 {
		if p.pretty {
			b.LF()
		}
		b.keyword("INHERITS")
		b.Append(p.printSubClauseInline(node.InhRelations))
	}
	opts := p.printNodes(node.Options, ",")
	if opts != "" {
		if p.pretty {
			b.LF()
		}
		b.keyword("WITH")
		b.Append(opts)
	}
	if node.Tablespacename != "" {
		if p.pretty {
			b.LF()
		}
		b.keyword("TABLESPACE")
		b.Append(node.Tablespacename)
	}
	return b.Join(" ")
}

func (p *printer) printDeleteStmt(node *DeleteStmt) string {
	b := p.builder()
	b.Append(p.printNode(node.WithClause))
	b.keyword("DELETE FROM")

	b.Append(p.printNode(node.Relation))
	if p.pretty {
		b.LF()
	}

	u := p.printNodes(node.UsingClause, ", ")
	if u != "" {
		b.keyword("USING")
		b.Append(u)
	}

	sub := p.printNode(node.WhereClause)
	if sub != "" {
		b.keyword("WHERE")
		if p.pretty {
			b.LF()
			sub = p.padLines(sub, 1)
		}
		b.Append(sub)
	}

	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
		if p.pretty {
			b.LF()
		}
		b.keyword("RETURNING")
		b.Append(r)
	}

	return b.Join(" ")
}

func (p *printer) printColumnDef(node *ColumnDef) string {
	b := p.builder()

	b.identifier(node.Colname)
	b.Append(p.printNode(node.TypeName))

	r := p.printNode(node.RawDefault)
	if r != "" {
		b.keyword("USING")
		b.Append(r)
	}

	b.Append(p.printNodes(node.Constraints, " "))

	if node.CollClause != nil {
		b.keyword("COLLATE")
		b.Append(p.printArr(node.CollClause.Collname)...)
	}

	return b.Join(" ")
}

func (p *printer) printTypeName(node *TypeName) string {
	b := p.builder()

	name := p.printNodes(node.Names, ".")

	if node.Setof {
		b.keyword("SETOF")
	}
	args := p.printNodes(node.Typmods, ", ")

	name = p.mapTypeName(name, args)
	for i := range node.ArrayBounds {
		bound := getInt32(node.ArrayBounds[i])
		if bound > 0 {
			name += "[" + strconv.Itoa(int(bound)) + "]"
		} else {
			name += "[]"
		}
	}
	if p.UpperType {
		name = strings.ToUpper(name)
	}

	b.Append(name)
	if name == "interval" && len(node.Typmods) > 0 {
		i := getInt32(node.Typmods[0])
		b.keyword(IntervalModType(i).String())
	}

	return b.Join(" ")
}

func getInt32(node Node) int32 {
	val, ok := node.(*Integer)
	if ok {
		return val.Ival
	}
	aConst, ok := node.(*AConst)
	if ok {
		val, ok := aConst.Val.(*Integer)
		if ok {
			return val.Ival
		}
	}
	return 0
}

func typeWrapper(name, args string) string {
	if args == "" {
		return name
	}
	return name + "(" + args + ")"
}

func (p *printer) mapTypeName(name, args string) string {
	if !strings.HasPrefix(name, "pg_catalog.") {
		return typeWrapper(name, args)
	}

	switch name[len("pg_catalog."):] {
	case "bpchar":
		return typeWrapper("char", args)
	case "varchar":
		return typeWrapper("varchar", args)
	case "numeric":
		return typeWrapper("numeric", args)
	case "bool":
		return "boolean"
	case "int2":
		return "smallint"
	case "int4":
		return "int"
	case "int8":
		return "bigint"
	case "real", "float4":
		return "real"
	case "float8":
		return "double precision"
	case "time":
		return "time"
	case "timetz":
		return "time with time zone"
	case "timestamp":
		return "timestamp"
	case "timestamptz":
		return "timestamp with time zone"
	case "interval":
		return "interval"
	default:
		p.addError(errors.New("Unknown data type: " + name))
	}
	return "**UNKNOWN TYPE**"
}

func (p *printer) printConstraint(node *Constraint) string {
	b := p.builder()

	if node.Conname != "" {
		b.keyword("CONSTRAINT")
		b.Append(node.Conname)
	}
	pre := ""
	post := ""
	switch node.Contype {
	case CONSTR_NULL:
		b.keyword("NULL")
	case CONSTR_NOTNULL:
		b.keyword("NOT NULL")
	case CONSTR_DEFAULT:
		b.keyword("DEFAULT")
	case CONSTR_CHECK:
		b.keyword("CHECK")
		pre = "("
		post = ")"
	case CONSTR_PRIMARY:
		b.keyword("PRIMARY KEY")
	case CONSTR_UNIQUE:
		b.keyword("UNIQUE")
	case CONSTR_EXCLUSION:
		b.keyword("EXCLUDE")
	case CONSTR_FOREIGN:
		if len(node.FkAttrs) > 1 {
			b.keyword("FOREIGN KEY")
		}
	}

	b.Append(pre + p.printNode(node.RawExpr) + post)
	b.Append(p.printSubClauseInline(node.Keys))
	b.Append(p.printSubClauseInline(node.FkAttrs))

	if node.Pktable != nil {
		b.keyword("REFERENCES")
		b.Append(p.printNode(node.Pktable), p.printSubClauseInline(node.PkAttrs))
	}

	if node.SkipValidation {
		b.keyword("NOT VALID")
	}
	if node.Indexname != "" {
		b.keyword("USING INDEX")
		b.Append(node.Indexname)
	}
	opts := p.printNodes(node.Options, ",")
	if opts != "" {
		b.keyword("WITH")
		b.Append(opts)
	}
	if len(node.Exclusions) > 0 {
		b.keyword("USING")
		b.Append(node.AccessMethod)
		for _, n := range node.Exclusions {
			nn, ok := n.(*List)
			if ok {
				b.Append("(" + p.printNodes(nn.Items, " WITH ") + ")")
			}
		}

	}
	return b.Join(" ")
}

func parseBool(s string) bool {
	return s == "t" || s == "'t'"
}

func (p *printer) printTypeCast(node *TypeCast) string {
	a := p.printNode(node.Arg)
	t := p.printNode(node.TypeName)
	if t == "boolean" {
		return strconv.FormatBool(parseBool(a))
	}
	return a + "::" + t
}

func (p *printer) printList(node *List) string {
	return p.printNodes(node.Items, ", ")
}

func (p *printer) printFuncCall(node *FuncCall) string {
	args := p.printNodes(node.Args, ", ")
	if node.AggStar {
		args = "*"
	}
	name := p.printNodes(node.Funcname, ".")
	distinct := ""
	if node.AggDistinct {
		distinct = "DISTINCT "
	}
	result := fmt.Sprintf("%s(%s%s)", name, distinct, args)
	if node.Over != nil {
		result += " " + p.printNode(node.Over)
	}
	return result
}

func (p *printer) printCreateSchemaStmt(node *CreateSchemaStmt) string {
	b := p.builder()
	b.keyword("CREATE SCHEMA")
	if node.IfNotExists {
		b.keyword("IF NOT EXISTS")
	}
	if node.Schemaname != "" {
		b.Append(p.identifier(node.Schemaname))
	}
	if node.Authrole != nil {
		b.keyword("AUTHORIZATION")
		b.Append(p.printNode(node.Authrole))
	}
	if len(node.SchemaElts) > 0 {
		b.Append(p.printNodes(node.SchemaElts, " "))
	}
	return b.Join(" ")
}

func (p *printer) printCaseExpr(node *CaseExpr) string {
	b := p.builder()
	b.keyword("CASE")

	b.Append(p.printNode(node.Arg))
	b.Append(p.printArr(node.Args)...)
	sub := p.printNode(node.Defresult)
	if sub != "" {
		if p.pretty {
			b.LF()
		}
		b.keyword("ELSE")
		b.Append(sub)
	}
	if p.pretty {
		b.LF()
	}
	b.Append("END")

	return b.Join(" ")
}

func (p *printer) printAArrayExpr(node *AArrayExpr) string {
	return fmt.Sprintf("%s[%s]", p.keyword("ARRAY"), p.printNodes(node.Elements, ", "))
}

func (p *printer) printCaseWhen(node *CaseWhen) string {
	b := p.builder()
	b.keyword("WHEN")
	b.Append(p.printNode(node.Expr))
	b.keyword("THEN")
	b.Append(p.printNode(node.Result))
	return b.Join(" ")
}

func (p *printer) printCoalesceExpr(node *CoalesceExpr) string {
	return fmt.Sprintf("%s(%s)", p.keyword("COALESCE"), p.printNodes(node.Args, ", "))
}

func stripQuote(s string) string {
	if s[0] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func quote(s string) string {
	return "'" + s + "'"
}

func doubleQuote(s string) string {
	return "\"" + s + "\""
}

func quoted(ss []string) []string {
	result := make([]string, len(ss))
	for i, v := range ss {
		result[i] = quote(v)
	}

	return result
}

func (p *printer) printCreateEnumStmt(node *CreateEnumStmt) string {
	b := p.builder()
	b.keyword("CREATE TYPE")
	b.Append(p.printNodes(node.TypeName, "."))
	b.keyword("AS ENUM")
	var vals []string
	for _, n := range node.Vals {
		s, ok := n.(*String)
		if ok {
			vals = append(vals, s.Str)
		} else {
			p.addError(errors.New("invalid enum value type: " + reflect.TypeOf(n).Name()))
		}
	}
	b.Append("(" + strings.Join(quoted(vals), ", ") + ")")
	return b.Join(" ")
}

func (e ObjectType) TypeName() string {
	switch e {
	case OBJECT_ACCESS_METHOD:
		return "ACCESS METHOD"
	case OBJECT_AGGREGATE:
		return "AGGREGATE"
	case OBJECT_AMOP:
		return "AMOP"
	case OBJECT_AMPROC:
		return "AMPROC"
	case OBJECT_ATTRIBUTE:
		return "ATTRIBUTE"
	case OBJECT_CAST:
		return "CAST"
	case OBJECT_COLUMN:
		return "COLUMN"
	case OBJECT_COLLATION:
		return "COLLATION"
	case OBJECT_CONVERSION:
		return "CONVERSION"
	case OBJECT_DATABASE:
		return "DATABASE"
	case OBJECT_DEFAULT:
		return "DEFAULT"
	case OBJECT_DEFACL:
		return "DEFAULT ACL"
	case OBJECT_DOMAIN:
		return "DOMAIN"
	case OBJECT_DOMCONSTRAINT:
		return "DOMCONSTRAINT"
	case OBJECT_EVENT_TRIGGER:
		return "EVENT TRIGGER"
	case OBJECT_EXTENSION:
		return "EXTENSION"
	case OBJECT_FDW:
		return "FOREIGN DATA WRAPPER"
	case OBJECT_FOREIGN_SERVER:
		return "SERVER"
	case OBJECT_FOREIGN_TABLE:
		return "FOREIGN TABLE"
	case OBJECT_FUNCTION:
		return "FUNCTION"
	case OBJECT_INDEX:
		return "INDEX"
	case OBJECT_LANGUAGE:
		return "LANGUAGE"
	case OBJECT_LARGEOBJECT:
		return "LARGEOBJECT"
	case OBJECT_MATVIEW:
		return "MATERIALIZED VIEW"
	case OBJECT_OPCLASS:
		return "OPERATOR CLASS"
	case OBJECT_OPERATOR:
		return "OPERATOR"
	case OBJECT_OPFAMILY:
		return "OPERATOR FAMILY"
	case OBJECT_POLICY:
		return "POLICY"
	case OBJECT_PROCEDURE:
		return "PROCEDURE"
	case OBJECT_PUBLICATION:
		return "PUBLICATION"
	case OBJECT_PUBLICATION_REL:
		return "OBJECT_PUBLICATION_REL"
	case OBJECT_ROLE:
		return "ROLE"
	case OBJECT_ROUTINE:
		return "ROUTINE"
	case OBJECT_RULE:
		return "RULE"
	case OBJECT_SCHEMA:
		return "SCHEMA"
	case OBJECT_SEQUENCE:
		return "SEQUENCE"
	case OBJECT_SUBSCRIPTION:
		return "SUBSCRIPTION"
	case OBJECT_STATISTIC_EXT:
		return "STATISTIC"
	case OBJECT_TABCONSTRAINT:
		return "TABLE CONSTRAINT"
	case OBJECT_TABLE:
		return "TABLE"
	case OBJECT_TABLESPACE:
		return "TABLESPACE"
	case OBJECT_TRANSFORM:
		return "TRANSFORM"
	case OBJECT_TRIGGER:
		return "TRIGGER"
	case OBJECT_TSCONFIGURATION:
		return "TEXT SEARCH CONFIGURATION"
	case OBJECT_TSDICTIONARY:
		return "TEXT SEARCH DICTIONARY"
	case OBJECT_TSPARSER:
		return "TEXT SEARCH PARSER"
	case OBJECT_TSTEMPLATE:
		return "TEXT SEARCH TEMPLATE"
	case OBJECT_TYPE:
		return "TYPE"
	case OBJECT_USER_MAPPING:
		return "USER MAPPING"
	case OBJECT_VIEW:
		return "VIEW"
	}
	return fmt.Sprintf("ObjectType(%d)", e)
}

func (p *printer) printCommentStmt(node *CommentStmt) string {
	b := p.builder()
	b.keyword("COMMENT ON")

	b.Append(node.Objtype.TypeName())
	b.identifier(strings.Split(p.printNode(node.Object), ",")...)
	b.keyword("IS")
	b.Append(quote(node.Comment))
	return b.Join(" ")
}

func (p *printer) printSubClauseCustom(prefix, suffix, sep string, nodes Nodes, allowPretty bool) string {
	if allowPretty && p.pretty {
		prefix += "\n"
		suffix = "\n" + suffix
		sep += "\n"
	}
	sub := p.printNodes(nodes, sep)
	if sub == "" {
		return ""
	}
	if allowPretty && p.pretty {
		sub = p.padLines(sub, 1)
	}

	return prefix + sub + suffix
}

func (p *printer) printSubClause(nodes Nodes) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, true)
}

func (p *printer) printSubClauseInline(nodes Nodes) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, false)
}

func (p *printer) printCompositeTypeStmt(node *CompositeTypeStmt) string {
	b := p.builder()
	b.keyword("CREATE TYPE")
	b.Append(p.printRangeVarInternal(node.Typevar, true))
	b.keyword("AS")
	b.Append(p.printSubClause(node.Coldeflist))
	return b.Join(" ")
}

func (p *printer) printCommonTableExpr(node *CommonTableExpr) string {
	b := p.builder()
	b.identifier(node.Ctename)
	b.Append(p.printSubClauseInline(node.Aliascolnames))
	b.keyword("AS")
	div := ""
	if p.pretty {
		div = "\n"
	}
	sub := p.printNode(node.Ctequery)
	if p.pretty {
		sub = p.padLines(sub, 1)
	}

	b.Append("(" + div + sub + div + ")")
	return b.Join(" ")
}

func (p *printer) printAlterTableStmt(node *AlterTableStmt) string {
	b := p.builder()
	b.keyword("ALTER")
	switch node.Relkind {
	case OBJECT_TABLE:
		b.keyword("TABLE")
	case OBJECT_VIEW:
		b.keyword("VIEW")
	}

	b.Append(p.printRangeVar(node.Relation), p.printNodes(node.Cmds, ", "))

	return b.Join(" ")
}

type CommandOption struct {
	command string
	option  string
}

var alterTableCommand = map[AlterTableType]CommandOption{
	AT_AddColumn:                 {"ADD", ""},
	AT_ColumnDefault:             {"ALTER", ""},
	AT_DropNotNull:               {"ALTER", "DROP NOT NULL"},
	AT_SetNotNull:                {"ALTER", "SET NOT NULL"},
	AT_SetStatistics:             {"ALTER", "SET STATISTICS"},
	AT_SetOptions:                {"ALTER", "SET"},
	AT_ResetOptions:              {"ALTER", "RESET"},
	AT_SetStorage:                {"ALTER", "SET STORAGE"},
	AT_DropColumn:                {"DROP", ""},
	AT_AddIndex:                  {"ADD INDEX", ""},
	AT_AddConstraint:             {"ADD", ""},
	AT_AlterConstraint:           {"ALTER CONSTRAINT", ""},
	AT_ValidateConstraint:        {"VALIDATE CONSTRAINT", ""},
	AT_DropConstraint:            {"DROP CONSTRAINT", ""},
	AT_AlterColumnType:           {"ALTER", "TYPE"},
	AT_AlterColumnGenericOptions: {"ALTER", "OPTIONS"},
}

func (p *printer) printAlterTableCmd(node *AlterTableCmd) string {
	b := p.builder()

	c, ok := alterTableCommand[node.Subtype]
	if ok {
		b.keyword(c.command)
	}
	// commands
	b.keywordIf("IF EXISTS", node.MissingOk)
	b.Append(p.identifier(node.Name))

	if ok && c.option != "" {
		b.keyword(c.option)
	}

	b.Append(p.printNode(node.Def))
	if node.Behavior == DROP_CASCADE {
		b.keyword("CASCADE")
	}
	return b.Join(" ")
}

func (p *printer) printRenameStmt(node *RenameStmt) string {
	b := p.builder()
	b.keyword("ALTER")

	switch node.RenameType {
	case OBJECT_TABCONSTRAINT, OBJECT_COLUMN:
		b.keyword("TABLE")
	default:
		b.keyword(node.RenameType.TypeName())
	}

	switch node.RenameType {
	case OBJECT_CONVERSION, OBJECT_COLLATION, OBJECT_TYPE, OBJECT_DOMCONSTRAINT, OBJECT_AGGREGATE, OBJECT_FUNCTION:
		b.Append(p.printNode(node.Object))
	case OBJECT_TABLE, OBJECT_TABCONSTRAINT, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_VIEW, OBJECT_COLUMN:
		b.Append(p.printNode(node.Relation))
	case OBJECT_TABLESPACE, OBJECT_RULE, OBJECT_TRIGGER:
		b.Append(node.Subname)
	}

	b.keyword("RENAME")

	switch node.RenameType {
	case OBJECT_TABCONSTRAINT, OBJECT_DOMCONSTRAINT:
		b.keyword("CONSTRAINT")
	case OBJECT_COLUMN:
		b.Append(node.Subname)
	}

	b.keyword("TO")
	b.identifier(node.Newname)
	return b.Join(" ")
}

func (p *printer) printAlterObjectSchemaStmt(node *AlterObjectSchemaStmt) string {
	b := p.builder()
	b.keyword("ALTER")
	b.keyword(node.ObjectType.TypeName())
	b.Append(p.printNode(node.Object))
	b.Append(p.printNode(node.Relation))
	b.keyword("SET SCHEMA")
	b.Append(p.identifier(node.Newschema))
	b.keywordIf("IF EXISTS", node.MissingOk)
	return b.Join(" ")
}

func (p *printer) printAlterEnumStmt(node *AlterEnumStmt) string {
	b := p.builder()
	b.keyword("ALTER TYPE")
	b.Append(p.printNodes(node.TypeName, "."))
	if node.OldVal != "" {
		b.keyword("RENAME VALUE")
		b.Append(quote(node.OldVal))
		b.keyword("TO")
		b.Append(quote(node.NewVal))
		return b.Join(" ")
	}

	b.keyword("ADD VALUE")
	b.keywordIf("IF NOT EXISTS", node.SkipIfNewValExists)
	b.Append(quote(node.NewVal))
	if node.NewValNeighbor != "" {
		b.keywordIfElse("AFTER", "BEFORE", node.NewValIsAfter)
		b.Append(quote(node.NewValNeighbor))
	}

	return b.Join(" ")
}

func (p *printer) printCreateFunctionStmt(node *CreateFunctionStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.keywordIf("OR REPLACE", node.Replace)
	b.keyword("FUNCTION")

	args := p.printSubClause(node.Parameters)
	if args == "" {
		args = "()"
	}
	b.identifier(p.printArr(node.Funcname)...)
	b.Append(args)
	b.keyword("RETURNS")
	b.Append(p.printNode(node.ReturnType))
	b.Append(p.printNodes(node.Options, " "))
	return b.Join(" ")
}

func (p *printer) printFunctionParameter(node *FunctionParameter) string {
	b := p.builder()
	b.identifier(node.Name)
	t := p.printNode(node.ArgType)
	d := p.printNode(node.Defexpr)
	if d != "" {
		t += "=" + d
	}
	b.Append(t)
	return b.Join(" ")
}

func (p *printer) printDefElem(node *DefElem) string {
	arg := p.printNode(node.Arg)
	switch node.Defname {
	case "as":
		wrapper := " "
		if p.pretty {
			wrapper = "\n"
		}
		if strings.Contains(arg, "'") {
			return p.keyword("AS") + wrapper + "$$" + wrapper + stripQuote(arg) + wrapper + "$$"
		}
		return p.keyword("AS") + wrapper + quote(arg)
	case "language":
		return p.keyword("LANGUAGE ") + arg
	case "analyze":
		return p.keyword("ANALYZE")
	case "verbose":
		return p.keyword("VERBOSE")
	case "costs":
		return p.keyword("COSTS")
	case "settings":
		return p.keyword("SETTINGS")
	case "buffers":
		return p.keyword("BUFFERS")
	case "wal":
		return p.keyword("WAL")
	case "timing":
		return p.keyword("TIMING")
	case "summary":
		return p.keyword("SUMMARY")
	case "format":
		return p.keyword("(FORMAT " + arg + ")")
	case "fillfactor":
		return p.keyword("(FILLFACTOR=" + arg + ")")
	case "strict":
		if arg == "1" {
			return p.keyword("STRICT")
		}
		return ""
	}
	return p.keyword(arg)
}

func (p *printer) printDropStmt(node *DropStmt) string {
	b := p.builder()
	b.keyword("DROP")
	b.keyword(node.RemoveType.TypeName())
	b.keywordIf("CONCURRENTLY", node.Concurrent)
	b.keywordIf("IF EXISTS", node.MissingOk)
	switch node.RemoveType {
	case OBJECT_CAST:
		b.Append(p.printSubClauseCustom("(", ")", p.keyword(" AS "), node.Objects, false))
	case OBJECT_FUNCTION, OBJECT_AGGREGATE, OBJECT_SCHEMA, OBJECT_EXTENSION:
		b.Append(p.printNodes(node.Objects, ","))
	case OBJECT_OPFAMILY, OBJECT_OPCLASS:
		o := p.printArr(node.Objects)
		if len(o) > 1 {
			b.identifier(o[1:]...)
		}
		b.keyword("USING")
		b.Append(o[0])
	case OBJECT_TRIGGER, OBJECT_RULE, OBJECT_POLICY:
		o := p.printArr(node.Objects)
		b.Append(o[len(o)-1:]...)
		b.keyword("ON")
		b.identifier(o[0 : len(o)-1]...)
	case OBJECT_TRANSFORM:
		b.keyword("FOR")
		o := p.printArr(node.Objects)
		b.Append(o[0])
		b.keyword("LANGUAGE")
		b.Append(o[1])
	default:
		b.Append(p.printNodes(node.Objects, ", "))
	}
	b.keywordIf("CASCADE", node.Behavior == DROP_CASCADE)

	return b.Join(" ")
}

func (p *printer) printObjectWithArgs(node *ObjectWithArgs) string {
	b := p.builder()
	b.identifier(p.printArr(node.Objname)...)
	if !node.ArgsUnspecified {
		b.Append(p.printSubClause(node.Objargs))
	}
	return b.Join("")
}

func (p *printer) printInsertStmt(node *InsertStmt) string {
	b := p.builder()
	b.Append(p.printNode(node.WithClause))
	b.keyword("INSERT INTO")
	b.Append(p.printNode(node.Relation))
	b.Append(p.printSubClause(node.Cols))
	b.Append(p.printNode(node.SelectStmt))
	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
		b.keyword("RETURNING")
		b.Append(r)
	}
	return b.Join(" ")
}

func (p *printer) printNamedArgExpr(node *NamedArgExpr) string {
	b := p.builder()
	b.identifier(node.Name)
	b.keyword("=>")
	b.Append(p.printNode(node.Arg))
	return b.Join(" ")
}

func (p *printer) printSubLink(node *SubLink) string {
	sub := "(" + p.printNode(node.Subselect) + ")"
	switch node.SubLinkType {
	case ANY_SUBLINK:
		return p.printNode(node.Testexpr) + p.keyword(" IN ") + sub
	case ALL_SUBLINK:
		return p.printNode(node.Testexpr) + " " + p.printNodes(node.OperName, " ") + p.keyword(" ALL ") + sub
	case EXISTS_SUBLINK:
		return p.keyword("EXISTS") + sub
	default:
		return sub
	}
}

func (p *printer) printBoolExpr(node *BoolExpr) string {
	b := p.builder()
	for _, n := range node.Args {
		bExpr, ok := n.(*BoolExpr)
		if ok && ((node.Boolop == AND_EXPR && bExpr.Boolop == OR_EXPR) || node.Boolop == OR_EXPR) {
			b.Append("(" + p.printNode(n) + ")")
		} else {
			b.Append(p.printNode(n))
		}
	}

	op := p.keyword("AND ")
	if node.Boolop == OR_EXPR {
		op = p.keyword("OR ")
	}
	if p.pretty {
		op = "\n" + op
	}

	return b.Join(op)
}

func (p *printer) printUpdateTargets(list Nodes) string {
	b := p.builder()
	var multi *MultiAssignRef
	var names []string
	for i := range list {
		n := list[i]
		node, ok := n.(*ResTarget)
		if ok {
			v, ok := node.Val.(*MultiAssignRef)
			if ok {
				multi = v
				names = append(names, p.identifier(node.Name))
			} else {
				b.Append(p.identifier(node.Name) + " = " + p.printNode(node.Val))
			}
		} else { // Can this ever hit?
			b.Append(p.printNode(n))
		}
	}
	if multi != nil {
		b.Append("(" + strings.Join(names, ", ") + ")" + " = " + p.printNode(multi))
	}
	return b.Join(", ")
}

func (p *printer) printUpdateStmt(node *UpdateStmt) string {
	b := p.builder()
	b.Append(p.printNode(node.WithClause))
	b.keyword("UPDATE")
	b.Append(p.printNode(node.Relation))
	if len(node.TargetList) > 0 {
		b.keyword("SET")
		b.Append(p.printUpdateTargets(node.TargetList))
	}

	w := p.printNode(node.WhereClause)
	if w != "" {
		b.keyword("WHERE")
		b.Append(w)
	}

	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
		b.keyword("RETURNING")
		b.Append(r)
	}
	return b.Join(" ")
}

func (p *printer) printCreateTableAsStmt(node *CreateTableAsStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.Append(node.Relkind.TypeName())
	b.identifier(p.printNode(node.Into))
	if node.Into.OnCommit != ONCOMMIT_NOOP {
		b.keyword("ON COMMIT")
		switch node.Into.OnCommit {
		case ONCOMMIT_DELETE_ROWS:
			b.keyword("DELETE ROWS")
		case ONCOMMIT_DROP:
			b.keyword("DROP")
		}
	}
	b.keyword("AS")
	b.Append(p.printNode(node.Query))
	b.keywordIf("IF NOT EXISTS", node.IfNotExists)

	return b.Join(" ")
}

func (p *printer) printIntoClause(node *IntoClause) string {
	return p.printNode(node.Rel)
}

func (p *printer) printSortBy(node *SortBy) string {
	b := p.builder()
	b.Append(p.printNode(node.Node))
	switch node.SortbyDir {
	case SORTBY_ASC:
		b.keyword("ASC")
	case SORTBY_DESC:
		b.keyword("DESC")
	}
	switch node.SortbyNulls {
	case SORTBY_NULLS_FIRST:
		b.keyword("NULLS FIRST")
	case SORTBY_NULLS_LAST:
		b.keyword("NULLS LAST")
	}
	return b.Join(" ")
}

func (p *printer) printCreateExtensionStmt(node *CreateExtensionStmt) string {
	b := p.builder()
	b.keyword("CREATE EXTENSION")
	b.Append(p.printNodes(node.Options, " "))
	b.keywordIf("IF NOT EXISTS", node.IfNotExists)
	b.identifier(node.Extname)
	return b.Join(" ")
}

func (p *printer) printRangeSubselect(node *RangeSubselect) string {
	b := p.builder()
	b.Append("(" + p.printNode(node.Subquery) + ")")
	a := p.printNode(node.Alias)
	if a != "" {
		b.Append(a)
	}
	return b.Join(" ")
}

func (p *printer) printTruncateStmt(node *TruncateStmt) string {
	b := p.builder()
	b.keyword("TRUNCATE")

	b.Append(p.printNodes(node.Relations, ", "))
	b.keywordIf("RESTART IDENTITY", node.RestartSeqs)
	b.keywordIf("CASCADE", node.Behavior == DROP_CASCADE)
	return b.Join(" ")
}

func (p *printer) printMultiAssignRef(node *MultiAssignRef) string {
	return p.printNode(node.Source)
}

func (p *printer) printRowExpr(node *RowExpr) string {
	switch node.RowFormat {
	case COERCE_IMPLICIT_CAST:
		return p.printNodes(node.Args, ", ")
	}
	return p.printSubClauseInline(node.Args)
}

func (p *printer) printExplainStmt(node *ExplainStmt) string {
	b := p.builder()
	b.keyword("EXPLAIN")
	b.Append(p.printNodes(node.Options, " "))
	if p.pretty {
		b.LF()
	}
	b.Append(p.printNode(node.Query))
	return b.Join(" ")
}

func (p *printer) printNullTest(node *NullTest) string {
	b := p.builder()
	b.Append(p.printNode(node.Xpr), p.printNode(node.Arg))
	b.keywordIfElse("IS NULL", "IS NOT NULL", node.Nulltesttype == IS_NULL)

	return b.Join(" ")
}

func (p *printer) printViewStmt(node *ViewStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.keywordIf("OR REPLACE", node.Replace)
	b.Append(p.printNodes(node.Options, " "))
	b.keyword("VIEW")
	b.Append(p.printNode(node.View))
	b.Append(p.printSubClauseInline(node.Aliases))
	b.keyword("AS")
	if p.pretty {
		b.LF()
	}
	b.Append(p.printNode(node.Query))

	switch node.WithCheckOption {
	case LOCAL_CHECK_OPTION:
		if p.pretty {
			b.LF()
		}
		b.keyword("WITH LOCAL CHECK OPTION")
	case CASCADED_CHECK_OPTION:
		if p.pretty {
			b.LF()
		}
		b.keyword("WITH CASCADED CHECK OPTION")
	}

	return b.Join(" ")
}

func (e SQLValueFunctionOp) Op() string {
	switch e {
	case SVFOP_CURRENT_DATE:
		return "current_date"
	case SVFOP_CURRENT_TIME:
		return "current_time"
	case SVFOP_CURRENT_TIME_N:
		return "current_time"
	case SVFOP_CURRENT_TIMESTAMP:
		return "current_timestamp"
	case SVFOP_CURRENT_TIMESTAMP_N:
		return "current_timestamp"
	case SVFOP_LOCALTIME:
		return "localtime"
	case SVFOP_LOCALTIME_N:
		return "localtime"
	case SVFOP_LOCALTIMESTAMP:
		return "localtimestamp"
	case SVFOP_LOCALTIMESTAMP_N:
		return "localtimestamp"
	case SVFOP_CURRENT_ROLE:
		return "current_role"
	case SVFOP_CURRENT_USER:
		return "current_user"
	case SVFOP_USER:
		return "user"
	case SVFOP_SESSION_USER:
		return "session_user"
	case SVFOP_CURRENT_CATALOG:
		return "current_catalog"
	case SVFOP_CURRENT_SCHEMA:
		return "current_schema"
	}
	return fmt.Sprintf("SQLValueFunctionOp(%d)", e)
}

func (p *printer) printSqlvalueFunction(node *SqlvalueFunction) string {
	return node.Op.Op()
}

func (p *printer) printIndexElem(node *IndexElem) string {
	return node.Name
}

func (p *printer) printCurrentOfExpr(node *CurrentOfExpr) string {
	return p.keyword("CURRENT OF ") + node.CursorName
}

func (p *printer) printRangeFunction(node *RangeFunction) string {
	b := p.builder()
	b.Append(p.printNodes(node.Functions, ", "))
	b.keywordIf("WITH ORDINALITY", node.Ordinality)
	if node.Alias != nil {
		b.keyword("AS")
		b.Append(p.printAlias(node.Alias))
	}
	b.Append(p.printSubClauseInline(node.Coldeflist))
	return b.Join(" ")
}
