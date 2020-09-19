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
	result := sqlBuilder{}
	sub := p.printWithClause(node.WithClause)
	result.Append(sub)

	switch node.Op {
	// TODO: Select Op handling?
	}
	if len(node.FromClause) > 0 || len(node.TargetList) > 0 {
		if p.pretty && sub != "" {
			result.LF()
		}
		result.Append("SELECT")
		if p.pretty {
			result.LF()
		}
	}

	if len(node.TargetList) > 0 {
		if len(node.DistinctClause) > 0 {
			result.Append("DISTINCT")
			columns := p.printNodes(node.DistinctClause, ", ")
			result.Append(fmt.Sprintf("ON (%s)", columns))
		}

		columns := p.printNodes(node.TargetList, ", ")
		if p.pretty {
			columns = p.padLines(columns, 1)
		}

		result.Append(columns)

		if node.IntoClause != nil {
			result.Append("INTO")
			result.Append(p.printNode(node.IntoClause))
		}
	}
	if len(node.FromClause) > 0 {
		if p.pretty {
			result.LF()
		}
		result.Append("FROM")
		columns := p.printNodes(node.FromClause, ", ")

		if p.pretty {
			result.LF()
			columns = p.padLines(columns, 1)
		}
		result.Append(columns)
	}

	if node.WhereClause != nil {
		if p.pretty {
			result.LF()
		}
		result.Append(p.keyword("WHERE"))
		sub := p.printNode(node.WhereClause)
		if p.pretty {
			result.LF()
			sub = p.padLines(sub, 1)
		}
		result.Append(sub)
	}

	if len(node.ValuesLists) > 0 {
		if p.pretty {
			result.LF()
		}
		result.Append("VALUES")
		if p.pretty {
			result.LF()
		}
		var vv []string
		for _, nl := range node.ValuesLists {
			vv = append(vv, fmt.Sprintf("(%s)", p.printNode(nl)))
		}
		if p.pretty {
			result.Append(p.padLines(strings.Join(vv, "\n"), 1))
		} else {
			result.Append(strings.Join(vv, " "))
		}
	}
	if len(node.GroupClause) > 0 {
		if p.pretty {
			result.LF()
		}
		result.Append("GROUP BY")
		result.Append(p.printNodes(node.GroupClause, ", "))
	}
	if node.HavingClause != nil {
		if p.pretty {
			result.LF()
		}
		result.Append("HAVING")
		result.Append(p.printNode(node.HavingClause))
	}
	if len(node.SortClause) > 0 {
		if p.pretty {
			result.LF()
		}
		result.Append("ORDER BY")
		result.Append(p.printNodes(node.SortClause, ", "))
	}
	if node.LimitCount != nil {
		if p.pretty {
			result.LF()
		}
		result.Append("LIMIT")
		result.Append(p.printNode(node.LimitCount))
	}
	if node.LimitOffset != nil {
		if p.pretty {
			result.LF()
		}
		result.Append("OFFSET")
		result.Append(p.printNode(node.LimitOffset))
	}
	if len(node.LockingClause) > 0 {
		result.Append(p.printNodes(node.LockingClause, " "))
	}

	return result.Join(" ")
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
	// TODO: Does this need to be encoded?
	return node.Str
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
	result := sqlBuilder{}
	result.Append(p.keyword("CREATE"))
	result.Append(p.relPersistence(node.Relation))
	result.Append(p.keyword("TABLE"))

	if node.IfNotExists {
		result.Append(p.keyword("IF NOT EXISTS"))
	}
	result.Append(p.printNode(node.Relation))

	sub := p.printSubClause(node.TableElts)
	if sub == "" {
		// Empty table definitions are valid
		sub = "()"
	}
	result.Append(sub)

	if len(node.InhRelations) > 0 {
		result.Append(p.keyword("INHERITS"))
		result.Append(p.printSubClause(node.InhRelations))
	}
	return result.Join(" ")
}

func (p *printer) printDeleteStmt(node *DeleteStmt) string {
	result := sqlBuilder{}
	result.Append(p.printNode(node.WithClause))
	result.Append(p.keyword("DELETE FROM"))

	result.Append(p.printNode(node.Relation))
	if p.pretty {
		result.LF()
	}

	u := p.printNodes(node.UsingClause, ", ")
	if u != "" {
		result.Append(p.keyword("USING"))
		result.Append(u)
	}

	sub := p.printNode(node.WhereClause)
	if sub != "" {
		result.Append(p.keyword("WHERE"))
		if p.pretty {
			result.LF()
			sub = p.padLines(sub, 1)
		}
		result.Append(sub)
	}

	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
		if p.pretty {
			result.LF()
		}
		result.Append(p.keyword("RETURNING"))
		result.Append(r)
	}

	return result.Join(" ")
}

func (p *printer) printColumnDef(node *ColumnDef) string {
	result := sqlBuilder{}

	result.identifier(node.Colname)
	result.Append(p.printNode(node.TypeName))

	r := p.printNode(node.RawDefault)
	if r != "" {
		result.Append(p.keyword("USING"))
		result.Append(r)
	}
	result.Append(p.printNodes(node.Constraints, " "))

	if node.CollClause != nil {
		result.Append(p.keyword("COLLATE"))
		result.Append(p.printNodes(node.CollClause.Collname, " "))
	}

	return result.Join(" ")
}

func (p *printer) printTypeName(node *TypeName) string {
	name := p.printNodes(node.Names, ".")

	result := sqlBuilder{}

	if node.Setof {
		result.Append(p.keyword("SETOF"))
	}
	args := p.printNodes(node.Typmods, ", ")

	name = p.mapTypeName(name, args)
	if len(node.ArrayBounds) > 0 {
		name += "[]"
	}
	if p.UpperType {
		name = strings.ToUpper(name)
	}

	result.Append(name)
	if name == "interval" && len(node.Typmods) > 0 {
		// TODO Special handling for interval with mods
	}

	return result.Join(" ")
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
	result := sqlBuilder{}

	if node.Conname != "" {
		result.Append(p.keyword("CONSTRAINT"), node.Conname)
	}
	switch node.Contype {
	case CONSTR_NULL:
		result.Append(p.keyword("NULL"))
	case CONSTR_NOTNULL:
		result.Append(p.keyword("NOT NULL"))
	case CONSTR_DEFAULT:
		result.Append(p.keyword("DEFAULT"))
	case CONSTR_CHECK:
		result.Append(p.keyword("CHECK"))
	case CONSTR_PRIMARY:
		result.Append(p.keyword("PRIMARY KEY"))
	case CONSTR_UNIQUE:
		result.Append(p.keyword("UNIQUE"))
	case CONSTR_EXCLUSION:
		result.Append(p.keyword("EXCLUSION"))
	case CONSTR_FOREIGN:
		if len(node.FkAttrs) > 1 {
			result.Append(p.keyword("FOREIGN KEY"))
		}
	}

	e := p.printNode(node.RawExpr)

	if e != "" {
		result.Append("(", e, ")")
	}

	sub := p.printNodes(node.Keys, ", ")
	if sub != "" {
		result.Append("(" + sub + ")")
	}
	sub = p.printNodes(node.FkAttrs, ", ")
	if sub != "" {
		result.Append("(" + sub + ")")
	}
	if node.Pktable != nil {
		result.Append(p.keyword("REFERENCES"), p.printNode(node.Pktable), "("+p.printNodes(node.PkAttrs, ", ")+")")
	}

	if node.SkipValidation {
		result.Append(p.keyword("NOT VALID"))
	}
	if node.Indexname != "" {
		result.Append(p.keyword("USING INDEX"), node.Indexname)
	}

	return result.Join(" ")
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
	result := sqlBuilder{}
	result.Append(p.keyword("CREATE SCHEMA"))
	if node.IfNotExists {
		result.Append(p.keyword("IF NOT EXISTS"))
	}
	if node.Schemaname != "" {
		result.Append(p.identifier(node.Schemaname))
	}
	if node.Authrole != nil {
		result.Append(p.keyword("AUTHORIZATION"))
		result.Append(p.printNode(node.Authrole))
	}
	if len(node.SchemaElts) > 0 {
		result.Append(p.printNodes(node.SchemaElts, " "))
	}
	return result.Join(" ")
}

func (p *printer) printCaseExpr(node *CaseExpr) string {
	result := sqlBuilder{}
	result.Append(p.keyword("CASE"))

	result.Append(p.printNode(node.Arg))
	result.Append(p.printNodes(node.Args, " "))
	sub := p.printNode(node.Defresult)
	if sub != "" {
		if p.pretty {
			result.LF()
		}
		result.Append(p.keyword("ELSE"))
		result.Append(sub)
	}
	if p.pretty {
		result.LF()
	}
	result.Append("END")

	return result.Join(" ")
}

func (p *printer) printAArrayExpr(node *AArrayExpr) string {
	return fmt.Sprintf("%s[%s]", p.keyword("ARRAY"), p.printNodes(node.Elements, ", "))
}

func (p *printer) printCaseWhen(node *CaseWhen) string {
	result := sqlBuilder{}
	result.Append(p.keyword("WHEN"))
	result.Append(p.printNode(node.Expr))
	result.Append(p.keyword("THEN"))
	result.Append(p.printNode(node.Result))
	return result.Join(" ")
}

func (p *printer) printCoalesceExpr(node *CoalesceExpr) string {
	return fmt.Sprintf("%s(%s)", p.keyword("COALESCE"), p.printNodes(node.Args, ", "))
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
	result := sqlBuilder{}
	result.Append(p.keyword("CREATE TYPE"))
	result.Append(p.printNodes(node.TypeName, "."))
	result.Append(p.keyword("AS ENUM"))
	var vals []string
	for _, n := range node.Vals {
		s, ok := n.(*String)
		if ok {
			vals = append(vals, s.Str)
		} else {
			p.addError(errors.New("invalid enum value type: " + reflect.TypeOf(n).Name()))
		}
	}
	result.Append("(" + strings.Join(quoted(vals), ", ") + ")")
	return result.Join(" ")
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
	result := sqlBuilder{}
	result.Append(p.keyword("COMMENT ON"))

	result.Append(node.Objtype.TypeName())
	result.Append(p.identifier(strings.Split(p.printNode(node.Object), ",")...))
	result.Append(p.keyword("IS"))
	result.Append(quote(node.Comment))
	return result.Join(" ")
}

func (p *printer) printSubClauseCustom(prefix, suffix, sep string, nodes Nodes) string {
	if p.pretty {
		prefix += "\n"
		suffix = "\n" + suffix
		sep += "\n"
	}
	sub := p.printNodes(nodes, sep)
	if sub == "" {
		return ""
	}
	if p.pretty {
		sub = p.padLines(sub, 1)
	}

	return prefix + sub + suffix
}

func (p *printer) printSubClause(nodes Nodes) string {
	return p.printSubClauseCustom("(", ")", ",", nodes)
}

func (p *printer) printCompositeTypeStmt(node *CompositeTypeStmt) string {
	result := sqlBuilder{}
	result.Append(p.keyword("CREATE TYPE"))
	result.Append(p.printRangeVarInternal(node.Typevar, true))
	result.Append(p.keyword("AS"), p.printSubClause(node.Coldeflist))
	return result.Join(" ")
}

func (p *printer) printCommonTableExpr(node *CommonTableExpr) string {
	result := sqlBuilder{}
	result.Append(node.Ctename)
	result.Append(p.printSubClause(node.Aliascolnames))
	result.Append(p.keyword("AS"))
	div := ""
	if p.pretty {
		div = "\n"
	}
	sub := p.printNode(node.Ctequery)
	if p.pretty {
		sub = p.padLines(sub, 1)
	}

	result.Append("(" + div + sub + div + ")")
	return result.Join(" ")
}

func (p *printer) printAlterTableStmt(node *AlterTableStmt) string {
	result := sqlBuilder{}
	result.Append(p.keyword("ALTER"))
	switch node.Relkind {
	case OBJECT_TABLE:
		result.Append(p.keyword("TABLE"))
	case OBJECT_VIEW:
		result.Append(p.keyword("VIEW"))

	}

	result.Append(p.printRangeVar(node.Relation), p.printNodes(node.Cmds, ", "))

	return result.Join(" ")
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
	result := sqlBuilder{}

	c, ok := alterTableCommand[node.Subtype]
	if ok {
		result.Append(p.keyword(c.command))
	}
	// commands
	if node.MissingOk {
		result.Append("IF EXISTS")
	}
	result.Append(p.identifier(node.Name))

	if ok && c.option != "" {
		result.Append(p.keyword(c.option))
	}

	result.Append(p.printNode(node.Def))
	if node.Behavior == DROP_CASCADE {
		result.Append(p.keyword("CASCADE"))
	}
	return result.Join(" ")
}

func (p *printer) printRenameStmt(node *RenameStmt) string {
	result := sqlBuilder{}
	result.Append(p.keyword("ALTER"))

	switch node.RenameType {
	case OBJECT_TABCONSTRAINT, OBJECT_COLUMN:
		result.Append(p.keyword("TABLE"))
	default:
		result.Append(p.keyword(node.RenameType.TypeName()))
	}
	switch node.RenameType {
	case OBJECT_CONVERSION, OBJECT_COLLATION, OBJECT_TYPE, OBJECT_DOMCONSTRAINT, OBJECT_AGGREGATE, OBJECT_FUNCTION:
		result.Append(p.printNode(node.Object))
	case OBJECT_TABLE, OBJECT_TABCONSTRAINT, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_VIEW, OBJECT_COLUMN:
		result.Append(p.printNode(node.Relation))
	case OBJECT_TABLESPACE, OBJECT_RULE, OBJECT_TRIGGER:
		result.Append(node.Subname)
	}

	result.Append(p.keyword("RENAME"))

	switch node.RenameType {
	case OBJECT_TABCONSTRAINT, OBJECT_DOMCONSTRAINT:
		result.Append(p.keyword("CONSTRAINT"))
	case OBJECT_COLUMN:
		result.Append(node.Subname)
	}

	result.Append(p.keyword("TO"))
	result.Append(p.identifier(node.Newname))
	return result.Join(" ")
}

func (p *printer) printAlterObjectSchemaStmt(node *AlterObjectSchemaStmt) string {
	result := sqlBuilder{}
	result.Append(p.keyword("ALTER"))
	result.Append(p.keyword(node.ObjectType.TypeName()))
	result.Append(p.printNode(node.Object))
	result.Append(p.printNode(node.Relation))
	result.Append(p.keyword("SET SCHEMA"))
	result.Append(p.identifier(node.Newschema))
	if node.MissingOk {
		result.Append(p.keyword("IF EXISTS"))
	}
	return result.Join(" ")
}

func (p *printer) printAlterEnumStmt(node *AlterEnumStmt) string {
	result := sqlBuilder{}
	result.Append(p.keyword("ALTER TYPE"))
	result.Append(p.printNodes(node.TypeName, "."))
	if node.OldVal != "" {
		result.Append(p.keyword("RENAME VALUE"))
		result.Append(quote(node.OldVal))
		result.Append(p.keyword("TO"))
		result.Append(quote(node.NewVal))
		return result.Join(" ")
	}

	result.Append(p.keyword("ADD VALUE"))
	if node.SkipIfNewValExists {
		result.Append(p.keyword("IF NOT EXISTS"))
	}
	result.Append(quote(node.NewVal))
	if node.NewValNeighbor != "" {
		if node.NewValIsAfter {
			result.Append(p.keyword("AFTER"))
		} else {
			result.Append(p.keyword("BEFORE"))
		}
		result.Append(quote(node.NewValNeighbor))
	}

	return result.Join(" ")
}

func (p *printer) printCreateFunctionStmt(node *CreateFunctionStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	if node.Replace {
		b.keyword("OR REPLACE")
	}
	b.keyword("FUNCTION")

	args := p.printSubClause(node.Parameters)
	if args == "" {
		args = "()"
	}
	b.Append(p.printNodes(node.Funcname, ".") + args)
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
		if strings.Contains(arg, "'") {
			return p.keyword("AS $$") + arg + "$$"
		}
		return p.keyword("AS ") + quote(arg)
	case "language":
		return p.keyword("LANGUAGE ") + arg
	//	TODO Check Volatility rules
	//case "volatility":
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
		b.Append(p.printSubClauseCustom("(", ")", p.keyword(" AS "), node.Objects))
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
	op := p.keyword("AND")
	if node.Boolop == OR_EXPR {
		op = p.keyword("OR")
	}

	for _, n := range node.Args {
		bExpr, ok := n.(*BoolExpr)
		if ok && ((node.Boolop == AND_EXPR && bExpr.Boolop == OR_EXPR) || node.Boolop == OR_EXPR) {
			b.Append("(" + p.printNode(n) + ")")
		} else {
			b.Append(p.printNode(n))
		}
	}
	switch node.Boolop {
	case AND_EXPR:
	case OR_EXPR:
	}

	return b.Join(" " + op + " ")
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
	// TODO when to include "Row"
	// TODO Handle CoercionForm
	return p.printSubClause(node.Args)
}
