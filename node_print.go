package pgtree

import (
	"fmt"
	"strconv"
	"strings"
)

func (p *printer) printJoinExpr(node *JoinExpr) string {
	b := p.builder()
	b.Append(p.printNode(node.Larg))
	b.LF()

	switch node.Jointype {
	case JOIN_INNER:
		if node.IsNatural {
			b.keyword("NATURAL")
		} else if node.Quals == nil && len(node.UsingClause) == 0 {
			b.keyword("CROSS")
		}
	case JOIN_LEFT:
		b.keyword("LEFT")
	case JOIN_FULL:
		b.keyword("FULL")
	case JOIN_RIGHT:
		b.keyword("RIGHT")
	}

	b.keyword("JOIN")
	b.Append(p.printNode(node.Rarg))

	if node.Quals != nil {
		b.keyword("ON")
		b.Append(p.printNode(node.Quals))
	}

	if len(node.UsingClause) > 0 {
		columns := p.printNodes(node.UsingClause, ", ")
		b.keyword("USING")
		b.Append(fmt.Sprintf("(%s)", columns))
	}

	return b.Join(" ")
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
		b.LF()
		b.keyword(SetOpUnionKeyword[node.Op])
		b.keywordIf("ALL", node.All)
		b.LF()
		b.Append(p.printNode(node.Rarg))
	}

	if len(node.FromClause) > 0 || len(node.TargetList) > 0 {
		if sub != "" {
			b.LF()
		}

		b.keyword("SELECT")
		b.LF()
	}

	if len(node.TargetList) > 0 {
		if len(node.DistinctClause) > 0 {
			b.keyword("DISTINCT ON")
			b.Append(p.printSubClauseInline(node.DistinctClause))
			b.LF()
		}

		sep := ", "
		if p.pretty && p.OneResultColumnPerLine {
			sep = ",\n"
		}
		b.AppendPadded(strings.Join(p.printArr(node.TargetList), sep))

		if node.IntoClause != nil {
			b.keyword("INTO")
			b.LF()
			b.AppendPadded(p.printNode(node.IntoClause))
		}
	}

	if len(node.FromClause) > 0 {
		b.keyword("FROM")
		b.Append(p.printCSV(node.FromClause))
	}

	if node.WhereClause != nil {
		b.keyword("WHERE")
		b.LF()
		b.AppendPadded(p.printNode(node.WhereClause))
	}

	if len(node.ValuesLists) > 0 {
		b.keyword("VALUES")
		b.LF()

		var vv []string
		for _, nl := range node.ValuesLists {
			vv = append(vv, fmt.Sprintf("(%s)", p.printNode(nl)))
		}

		if p.pretty {
			b.Append(p.padLines(strings.Join(vv, ",\n")))
		} else {
			b.Append(strings.Join(vv, ", "))
		}
	}

	if len(node.GroupClause) > 0 {
		b.keyword("GROUP BY")
		b.Append(p.printNodes(node.GroupClause, ", "))
		b.LF()
	}

	if node.HavingClause != nil {
		b.keyword("HAVING")
		b.Append(p.printNode(node.HavingClause))
		b.LF()
	}

	if len(node.SortClause) > 0 {
		b.keyword("ORDER BY")
		b.Append(p.printNodes(node.SortClause, ", "))
		b.LF()
	}

	if node.LimitCount != nil {
		b.keyword("LIMIT")
		b.Append(p.printNode(node.LimitCount))
		b.LF()
	}

	if node.LimitOffset != nil {
		b.keyword("OFFSET")
		b.Append(p.printNode(node.LimitOffset))
		b.LF()
	}

	if len(node.LockingClause) > 0 {
		b.Append(p.printNodes(node.LockingClause, " "))
		b.LF()
	}

	if len(node.WindowClause) > 0 {
		b.keyword("WINDOW")
		b.Append(p.printNodes(node.WindowClause, " "))
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

		return fmt.Sprintf("%s %s (%s)", left, p.keyword(op), right)
	case AEXPR_LIKE:
		if op == "~~" {
			op = "LIKE"
		} else {
			op = "NOT LIKE"
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword(op), right)
	case AEXPR_ILIKE:
		if op == "~~*" {
			op = "ILIKE"
		} else {
			op = "NOT ILIKE"
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword(op), right)
	case AEXPR_SIMILAR:
		fc, ok := node.Rexpr.(*FuncCall)
		if ok {
			name := p.printNodes(fc.Funcname, ".")
			if name == "pg_catalog.similar_escape" && len(fc.Args) == 2 {
				if p.printNode(fc.Args[1]) == "NULL" {
					right = p.printNode(fc.Args[0])
				}
			}
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword("SIMILAR TO"), right)
	case AEXPR_BETWEEN:
		l := node.Rexpr.(*List)
		low := p.printNode(l.Items[0])
		high := p.printNode(l.Items[1])

		return fmt.Sprintf("%s %s %s %s %s", left, p.keyword("BETWEEN"), low, p.keyword("AND"), high)
	}

	p.addError(ErrPrinter.Wrap("unhandled A_Expr kind type: " + node.Kind.String()))

	return ""
}

func (p *printer) printRangeVar(node *RangeVar) string {
	return p.printRangeVarInternal(node, false)
}

func (p *printer) printRangeVarInternal(node *RangeVar, ignoreInh bool) string {
	b := p.builder()

	if !node.Inh && !ignoreInh {
		b.keyword("ONLY")
	}

	schema := ""
	if node.Schemaname != "" {
		schema = p.identifier(node.Schemaname) + "."
	}

	b.Append(schema + p.identifier(node.Relname))

	if node.Alias != nil {
		b.Append(p.printAlias(node.Alias))
	}

	return b.Join(" ")
}

func (p *printer) printAlias(node *Alias) string {
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
	out := p.printNode(node.Stmt)
	if out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}

	return out + term
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
		return p.keyword("TEMP")
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

	name := p.printNode(node.Relation)

	if node.OfTypename != nil {
		name = name + p.keyword(" OF ") + p.identifier(p.printTypeName(node.OfTypename))
	}

	b.Append(name)

	sub := p.printSubClause(node.TableElts)
	if sub == "" {
		// Empty table definitions are valid
		sub = "()"
	}

	b.AddToLast(sub)

	if len(node.InhRelations) > 0 {
		b.LF()
		b.keyword("INHERITS")
		b.Append(p.printSubClauseInline(node.InhRelations))
	}

	opts := p.printNodes(node.Options, ",")
	if opts != "" {
		b.LF()
		b.keyword("WITH")
		b.Append(opts)
	}

	if node.Tablespacename != "" {
		b.LF()
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
	b.LF()

	u := p.printNodes(node.UsingClause, ", ")
	if u != "" {
		b.keyword("USING")
		b.Append(u)
		b.LF()
	}

	sub := p.printNode(node.WhereClause)
	if sub != "" {
		b.keyword("WHERE")
		b.LF()
		b.AppendPadded(sub)
	}

	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
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

const keywordInterval = "interval"

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
	if name == keywordInterval && len(node.Typmods) > 0 {
		i := getInt32(node.Typmods[0])
		b.keyword(IntervalModType(i).String())
		if len(node.Typmods) > 1 {
			// Precision
			i := getInt32(node.Typmods[1])
			b.Append("(" + strconv.Itoa(int(i)) + ")")
		}
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
	case keywordInterval:
		return keywordInterval
	}

	p.addError(ErrPrinter.Wrap("Unknown data type: " + name))

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
	b.Append(p.printSubClauseInlineSpace(node.Keys))
	b.Append(p.printSubClauseInlineSpace(node.FkAttrs))

	if node.Pktable != nil {
		b.keyword("REFERENCES")
		b.Append(p.printNode(node.Pktable), p.printSubClauseInlineSpace(node.PkAttrs))
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
		result += p.keyword(" OVER ") + p.printNode(node.Over)
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
		b.Append(p.printSpaced(node.SchemaElts))
	}

	return b.Join(" ")
}

func (p *printer) printCaseExpr(node *CaseExpr) string {
	b := p.builder()
	b.keyword("CASE")
	b.Append(p.printNode(node.Arg))
	whens := p.printSpaced(node.Args)
	b.Append(whens)
	sub := p.printNode(node.Defresult)
	if sub != "" {
		b.AppendPadded(p.keyword("ELSE ") + sub)
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
	if len(s) > 1 && s[0] == '"' {
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
		}
	}
	b.Append("(" + strings.Join(quoted(vals), ", ") + ")")

	return b.Join(" ")
}

func (p *printer) printCommentStmt(node *CommentStmt) string {
	b := p.builder()
	b.keyword("COMMENT ON")
	b.keyword(ObjectTypeTypeName[node.Objtype])

	switch n := node.Object.(type) {
	case *String:
		b.Append(n.Str)
	case *TypeName:
		b.Append(p.printTypeName(n))
	case *List:
		b.identifier(p.printArr(n.Items)...)
	}

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
		sub = p.padLines(sub)
	}

	return prefix + sub + suffix
}

func (p *printer) printSubClause(nodes Nodes) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, true)
}

func (p *printer) printCSV(nodes Nodes) string {
	return p.printSubClauseCustom("", "", ",", nodes, true)
}

func (p *printer) printSpaced(nodes Nodes) string {
	return p.printSubClauseCustom("", "", " ", nodes, true)
}

func (p *printer) printSubClauseInline(nodes Nodes) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, false)
}

func (p *printer) printSubClauseInlineSpace(nodes Nodes) string {
	return p.printSubClauseCustom("(", ")", ", ", nodes, false)
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
	b.Append(p.identifier(node.Ctename) + p.printSubClauseInlineSpace(node.Aliascolnames))
	b.keyword("AS")
	b.Append("(")
	b.LF()
	b.AppendPadded(p.printNode(node.Ctequery))
	b.Append(")")

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

	b.keywordIf("IF EXISTS", node.MissingOk)
	b.Append(p.printRangeVar(node.Relation))
	b.Append(p.printCSV(node.Cmds))

	return b.Join(" ")
}

type CommandOption struct {
	command string
	option  string
}

var alterTableCommand = map[AlterTableType]CommandOption{
	AT_AddColumn:                 {"ADD", ""},
	AT_ColumnDefault:             {"ALTER", "SET DEFAULT"},
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
	AT_ChangeOwner:               {"OWNER TO", ""},
	AT_SetRelOptions:             {"SET", ""},
	AT_ResetRelOptions:           {"RESET", ""},
}

func (p *printer) printAlterTableCmd(node *AlterTableCmd) string {
	b := p.builder()

	c := alterTableCommand[node.Subtype]
	if c.command != "" {
		b.keyword(c.command)
	}

	b.keywordIf("IF EXISTS", node.MissingOk)
	b.Append(p.identifier(node.Name))

	if node.Newowner != nil {
		b.Append(node.Newowner.Rolename)
	}

	def := p.printNode(node.Def)
	if node.Subtype == AT_ColumnDefault && def == "" {
		c.option = "DROP DEFAULT"
	}

	if c.option != "" {
		b.keyword(c.option)
	}

	b.Append(def)

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
		b.keyword(ObjectTypeTypeName[node.RenameType])
	}

	switch node.RenameType {
	case OBJECT_CONVERSION, OBJECT_COLLATION, OBJECT_TYPE, OBJECT_DOMCONSTRAINT, OBJECT_AGGREGATE, OBJECT_FUNCTION:
		b.Append(p.printNode(node.Object))
	case OBJECT_TABLE, OBJECT_TABCONSTRAINT, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_VIEW, OBJECT_COLUMN:
		b.Append(p.printNode(node.Relation))
	case OBJECT_TABLESPACE, OBJECT_RULE, OBJECT_TRIGGER:
		b.Append(node.Subname)
		b.keyword("ON")
		b.Append(p.printRangeVar(node.Relation))
	}

	b.keyword("RENAME")

	switch node.RenameType {
	case OBJECT_TABCONSTRAINT, OBJECT_DOMCONSTRAINT:
		b.keyword("CONSTRAINT")
		b.Append(node.Subname)
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
	b.keyword(ObjectTypeTypeName[node.ObjectType])
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
	b.identifier(p.printArr(node.Funcname)...)

	args := p.printSubClauseInlineSpace(node.Parameters)
	if args == "" {
		args = "()"
	}

	b.AddToLast(args)
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

		return p.keyword("AS") + wrapper + quote(stripQuote(arg))
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
		return p.keyword("FORMAT " + arg)
	case "fillfactor":
		return p.keyword("(FILLFACTOR=" + arg + ")")
	case "schema":
		return p.keyword("SCHEMA ") + p.identifier(arg)
	case "new_version":
		return p.keyword("VERSION ") + p.identifier(arg)
	case "old_version":
		return p.keyword("FROM ") + p.identifier(arg)
	case "user_catalog_table":
		return p.keyword("user_catalog_table")
	case "oids":
		if arg != "" {
			return p.keyword("oids=") + stripQuote(arg)
		}

		return p.keyword("oids")
	case "strict":
		return p.keyword("STRICT")
	}

	return p.keyword(arg)
}

func (p *printer) printBinaryList(nn []Node, sep string, invert bool) string {
	list := nn[0].(*List)
	o := p.printArr(list.Items)
	left := o[0]
	right := p.identifier(o[1:]...)

	if invert {
		return right + " " + sep + " " + left
	}

	return left + " " + sep + " " + right
}

func (p *printer) printDropStmt(node *DropStmt) string {
	b := p.builder()
	b.keyword("DROP")
	b.keyword(ObjectTypeTypeName[node.RemoveType])
	b.keywordIf("CONCURRENTLY", node.Concurrent)
	b.keywordIf("IF EXISTS", node.MissingOk)

	switch node.RemoveType {
	case OBJECT_CAST:
		tt := node.Objects[0].(*List)
		b.Append(p.printSubClauseCustom("(", ")", p.keyword(" AS "), tt.Items, false))
	case OBJECT_FUNCTION, OBJECT_AGGREGATE, OBJECT_SCHEMA, OBJECT_EXTENSION:
		b.Append(p.printNodes(node.Objects, ","))
	case OBJECT_OPFAMILY, OBJECT_OPCLASS:
		b.Append(p.printBinaryList(node.Objects, p.keyword("USING"), true))
	case OBJECT_TRIGGER, OBJECT_RULE, OBJECT_POLICY:
		b.Append(p.printBinaryList(node.Objects, p.keyword("ON"), true))
	case OBJECT_TRANSFORM:
		b.keyword("FOR")
		b.Append(p.printBinaryList(node.Objects, p.keyword("LANGUAGE"), false))
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
		b.Append(p.printSubClauseInlineSpace(node.Objargs))
	}

	return b.Join("")
}

func (p *printer) printInsertStmt(node *InsertStmt) string {
	b := p.builder()
	b.Append(p.printNode(node.WithClause))
	b.keyword("INSERT INTO")
	b.Append(p.printNode(node.Relation) + p.printSubClauseInlineSpace(node.Cols))
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
	} else {
		op = " " + op
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
	b.LF()
	b.keyword("SET")
	b.LF()
	b.AppendPadded(p.printUpdateTargets(node.TargetList))

	w := p.printNode(node.WhereClause)
	if w != "" {
		b.keyword("WHERE")
		b.LF()
		b.AppendPadded(w)
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
	b.Append(p.relPersistence(node.Into.Rel))
	b.keyword(ObjectTypeTypeName[node.Relkind])
	b.keywordIf("IF NOT EXISTS", node.IfNotExists)
	b.identifier(p.printNode(node.Into))
	b.Append(p.printSubClauseInlineSpace(node.Into.ColNames))
	b.LF()

	opts := p.printSubClauseInline(node.Into.Options)
	if opts != "" {
		b.keyword("WITH")
		b.Append(opts)
		b.LF()
	}

	if node.Into.OnCommit > ONCOMMIT_PRESERVE_ROWS {
		b.keyword("ON COMMIT")
		switch node.Into.OnCommit {
		case ONCOMMIT_DELETE_ROWS:
			b.keyword("DELETE ROWS")
		case ONCOMMIT_DROP:
			b.keyword("DROP")
		}

		b.LF()
	}

	if node.Into.TableSpaceName != "" {
		b.keyword("TABLESPACE")
		b.Append(node.Into.TableSpaceName)
		b.LF()
	}

	b.keyword("AS")
	b.LF()
	b.Append(p.printNode(node.Query))

	if node.Into.SkipData {
		b.LF()
		b.keyword("WITH NO DATA")
	}

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
	b.keywordIf("IF NOT EXISTS", node.IfNotExists)
	b.identifier(node.Extname)
	opts := p.printNodes(node.Options, " ")
	if opts != "" {
		b.LF()
		b.AppendPadded(p.keyword("WITH ") + opts)
	}

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
	return "(" + p.printNode(node.Source) + ")"
}

func (p *printer) printRowExpr(node *RowExpr) string {
	return p.printNodes(node.Args, ", ")
}

func (p *printer) printExplainStmt(node *ExplainStmt) string {
	b := p.builder()
	b.keyword("EXPLAIN")
	b.Append(p.printSubClauseInline(node.Options))
	b.LF()
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
	b.Append(p.printNode(node.View) + p.printSubClauseInlineSpace(node.Aliases))
	b.keyword("AS")
	b.LF()
	b.Append(p.printNode(node.Query))

	switch node.WithCheckOption {
	case LOCAL_CHECK_OPTION:
		b.keyword("WITH LOCAL CHECK OPTION")
	case CASCADED_CHECK_OPTION:
		b.keyword("WITH CASCADED CHECK OPTION")
	}

	return b.Join(" ")
}

func (p *printer) printSqlvalueFunction(node *SqlvalueFunction) string {
	return SQLValueFunctionOpLabel[node.Op]
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

	b.Append(p.printSubClauseInlineSpace(node.Coldeflist))

	return b.Join(" ")
}

func (p *printer) printLockingClause(node *LockingClause) string {
	b := p.builder()
	b.keyword("FOR " + LockClauseStrengthKeyword[node.Strength])

	switch node.WaitPolicy {
	case LockWaitError:
		b.keyword("NOWAIT")
	}

	return b.Join(" ")
}

func (p *printer) printLockStmt(node *LockStmt) string {
	b := p.builder()
	b.keyword("LOCK")
	b.Append(p.printNodes(node.Relations, ", "))
	b.keyword(LockModeKeyword[LockMode(node.Mode)])
	b.keywordIf("NOWAIT", node.Nowait)

	return b.Join(" ")
}

func (p *printer) printSetToDefault(_ *SetToDefault) string {
	return "DEFAULT"
}

func (p *printer) printCreateCastStmt(node *CreateCastStmt) string {
	b := p.builder()
	b.keyword("CREATE CAST")
	b.Append("(" + p.printTypeName(node.Sourcetype) + " AS " + p.printTypeName(node.Targettype) + ")")

	switch node.Context {
	case COERCION_IMPLICIT:
		b.keyword("WITHOUT FUNCTION AS IMPLICIT")
	case COERCION_ASSIGNMENT:
		b.keyword("WITH FUNCTION")
		b.Append(p.printNode(node.Func))
		b.keyword("AS ASSIGNMENT")
	case COERCION_EXPLICIT:
		b.keyword("WITH INOUT")
	}

	return b.Join(" ")
}

func (p *printer) printCreateOpClassStmt(node *CreateOpClassStmt) string {
	b := p.builder()
	b.keyword("CREATE OPERATOR CLASS")
	b.Append(p.printNodes(node.Opclassname, ", "))
	b.keywordIf("DEFAULT", node.IsDefault)
	b.keyword("FOR TYPE")
	b.Append(p.printTypeName(node.Datatype))
	b.keyword("USING")
	b.Append(node.Amname)
	b.keyword("AS")
	b.Append(p.printCSV(node.Items))

	return b.Join(" ")
}

const (
	OperatorItemType = 1
	FunctionItemType = 2
)

func (p *printer) printCreateOpClassItem(node *CreateOpClassItem) string {
	b := p.builder()
	switch node.Itemtype {
	case OperatorItemType:
		b.keyword("OPERATOR")
	case FunctionItemType:
		b.keyword("FUNCTION")
	}

	b.Append(strconv.Itoa(int(node.Number)))
	b.Append(p.printObjectWithArgs(node.Name))

	return b.Join(" ")
}

func (p *printer) printWindowDef(node *WindowDef) string {
	b := p.builder()
	if node.Name != "" {
		b.identifier(node.Name)
	}

	if len(node.PartitionClause) > 0 {
		if node.Name != "" {
			b.keyword("AS")
		}

		b.keyword("(PARTITION BY")
		b.Append(p.printNodes(node.PartitionClause, ", "))

		if len(node.OrderClause) > 0 {
			b.keyword("ORDER BY")
			b.Append(p.printNodes(node.OrderClause, ", "))
		}

		b.AddToLast(")")
	} else if node.Name == "" {
		b.Append("()")
	}

	return b.Join(" ")
}

func (p *printer) printRoleSpec(node *RoleSpec) string {
	return node.Rolename
}

func (p *printer) printRuleStmt(node *RuleStmt) string {
	b := p.builder()
	b.keyword("CREATE RULE")
	b.identifier(node.Rulename)
	b.keyword("AS")
	b.keyword("ON")
	b.keyword(CmdTypeKeyword[node.Event])
	b.keyword("TO")
	b.identifier(p.printRangeVar(node.Relation))

	if node.Instead {
		b.keyword("DO INSTEAD")
		b.Append(p.printNodes(node.Actions, ", "))
	} else {
		b.keyword("DO ALSO NOTIFY")
		b.Append(p.printNodes(node.Actions, ", "))
	}

	return b.Join(" ")
}

func (p *printer) printNotifyStmt(node *NotifyStmt) string {
	return node.Conditionname
}

func (p *printer) printCreateTransformStmt(node *CreateTransformStmt) string {
	b := p.builder()
	b.keyword("CREATE TRANSFORM FOR")
	b.identifier(p.printTypeName(node.TypeName))
	b.keyword("LANGUAGE")
	b.identifier(node.Lang)
	b.Append("(")
	b.LF()
	b.keyword("FROM SQL WITH FUNCTION")
	b.Append(p.printObjectWithArgs(node.Fromsql) + ", ")
	b.LF()
	b.keyword("TO SQL WITH FUNCTION")
	b.Append(p.printObjectWithArgs(node.Tosql))
	b.LF()
	b.Append(")")

	return b.Join(" ")
}
