package pgtree

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gofoji/pgtree/nodes"
)

func (p *printer) printJoinExpr(node *nodes.JoinExpr) string {
	b := p.builder()
	b.append(p.printNode(node.Larg))
	b.LF()

	switch node.Jointype {
	case nodes.JOIN_INNER:
		if node.IsNatural {
			b.keyword("NATURAL")
		} else if node.Quals == nil && len(node.UsingClause) == 0 {
			b.keyword("CROSS")
		}
	case nodes.JOIN_LEFT:
		b.keyword("LEFT")
	case nodes.JOIN_FULL:
		b.keyword("FULL")
	case nodes.JOIN_RIGHT:
		b.keyword("RIGHT")
	}

	b.keyword("JOIN")
	b.append(p.printNode(node.Rarg))

	if node.Quals != nil {
		b.keyword("ON")
		b.append(p.printNode(node.Quals))
	}

	if len(node.UsingClause) > 0 {
		columns := p.printNodes(node.UsingClause, ", ")

		b.keyword("USING")
		b.append(fmt.Sprintf("(%s)", columns))
	}

	return b.join(" ")
}

func (p *printer) printResTarget(node *nodes.ResTarget) string {
	if node.Name != "" {
		v := p.printNode(node.Val)
		if v != "" {
			return fmt.Sprintf("%s AS %s", v, p.identifier(node.Name))
		}

		return p.identifier(node.Name)
	}

	return p.printNode(node.Val)
}

func (p *printer) printColumnRef(node *nodes.ColumnRef) string {
	b := p.builder()
	b.identifier(p.printArr(node.Fields)...)

	return b.join(" ")
}

func (p *printer) printWithClause(node *nodes.WithClause) string {
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

func (p *printer) printSelectStmt(node *nodes.SelectStmt) string {
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

func (p *printer) printSelectStmtInternal(node *nodes.SelectStmt) string {
	b := p.builder()
	sub := p.printWithClause(node.WithClause)
	b.append(sub)

	if node.Op != nodes.SETOP_NONE {
		b.append(p.printNode(node.Larg))
		b.LF()
		b.keyword(nodes.SetOpKeyword[node.Op])
		b.keywordIf("ALL", node.All)
		b.LF()
		b.append(p.printNode(node.Rarg))
	}

	if len(node.FromClause) > 0 || len(node.TargetList) > 0 {
		if sub != "" {
			b.LF()
		}

		b.keyword("SELECT")
		b.LF()
	}

	p.printSelectTargets(node, &b)

	if len(node.FromClause) > 0 {
		b.keyword("FROM")
		b.append(p.printCSV(node.FromClause))
	}

	if node.WhereClause != nil {
		b.keyword("WHERE")
		b.LF()
		b.appendPadded(p.printNode(node.WhereClause))
	}

	p.printSelectValues(node, &b)
	p.printSelectCommonClauses(node, &b)

	if len(node.LockingClause) > 0 {
		b.append(p.printNodes(node.LockingClause, " "))
		b.LF()
	}

	if len(node.WindowClause) > 0 {
		b.keyword("WINDOW")
		b.append(p.printNodes(node.WindowClause, " "))
	}

	return b.join(" ")
}

func (p *printer) printSelectCommonClauses(node *nodes.SelectStmt, b *sqlBuilder) {
	if len(node.GroupClause) > 0 {
		b.keyword("GROUP BY")
		b.append(p.printNodes(node.GroupClause, ", "))
		b.LF()
	}

	if node.HavingClause != nil {
		b.keyword("HAVING")
		b.append(p.printNode(node.HavingClause))
		b.LF()
	}

	if len(node.SortClause) > 0 {
		b.keyword("ORDER BY")
		b.append(p.printNodes(node.SortClause, ", "))
		b.LF()
	}

	if node.LimitCount != nil {
		b.keyword("LIMIT")
		b.append(p.printNode(node.LimitCount))
		b.LF()
	}

	if node.LimitOffset != nil {
		b.keyword("OFFSET")
		b.append(p.printNode(node.LimitOffset))
		b.LF()
	}
}

func (p *printer) printSelectValues(node *nodes.SelectStmt, b *sqlBuilder) {
	if len(node.ValuesLists) > 0 {
		b.keyword("VALUES")
		b.LF()

		var vv []string
		for _, nl := range node.ValuesLists {
			vv = append(vv, fmt.Sprintf("(%s)", p.printNode(nl)))
		}

		if p.pretty {
			b.append(p.padLines(strings.Join(vv, ",\n")))
		} else {
			b.append(strings.Join(vv, ", "))
		}
	}
}

func (p *printer) printSelectTargets(node *nodes.SelectStmt, b *sqlBuilder) {
	if len(node.TargetList) == 0 {
		return
	}

	if len(node.DistinctClause) > 0 {
		b.keyword("DISTINCT ON")
		b.append(p.printSubClauseInline(node.DistinctClause))
		b.LF()
	}

	sep := ", "
	if p.pretty && p.OneResultColumnPerLine {
		sep = ",\n"
	}

	b.appendPadded(strings.Join(p.printArr(node.TargetList), sep))

	if node.IntoClause != nil {
		b.keyword("INTO")
		b.LF()
		b.appendPadded(p.printNode(node.IntoClause))
	}
}

func (p *printer) printAExpr(node *nodes.AExpr) string {
	left := p.printNode(node.Lexpr)
	right := p.printNode(node.Rexpr)
	op := p.printNodes(node.Name, " ")

	switch node.Kind {
	case nodes.AEXPR_OP:
		return fmt.Sprintf("%s %s %s", left, op, right)
	case nodes.AEXPR_OP_ANY:
		return fmt.Sprintf("%s %s ANY(%s)", left, op, right)
	case nodes.AEXPR_IN:
		if op == "=" {
			op = "IN"
		} else {
			op = "NOT IN"
		}

		return fmt.Sprintf("%s %s (%s)", left, p.keyword(op), right)
	case nodes.AEXPR_LIKE:
		if op == "~~" {
			op = "LIKE"
		} else {
			op = "NOT LIKE"
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword(op), right)
	case nodes.AEXPR_ILIKE:
		if op == "~~*" {
			op = "ILIKE"
		} else {
			op = "NOT ILIKE"
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword(op), right)
	case nodes.AEXPR_SIMILAR:
		fc, ok := node.Rexpr.(*nodes.FuncCall)
		if ok {
			name := p.printNodes(fc.Funcname, ".")
			if name == "pg_catalog.similar_escape" && len(fc.Args) == 2 {
				if p.printNode(fc.Args[1]) == "NULL" {
					right = p.printNode(fc.Args[0])
				}
			}
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword("SIMILAR TO"), right)
	case nodes.AEXPR_BETWEEN:
		l := node.Rexpr.(*nodes.List)
		low := p.printNode(l.Items[0])
		high := p.printNode(l.Items[1])

		return fmt.Sprintf("%s %s %s %s %s", left, p.keyword("BETWEEN"), low, p.keyword("AND"), high)
	}

	p.addError(ErrPrinter.Wrap("unhandled A_Expr kind type: " + node.Kind.String()))

	return ""
}

func (p *printer) printRangeVar(node *nodes.RangeVar) string {
	return p.printRangeVarInternal(node, false)
}

func (p *printer) printRangeVarInternal(node *nodes.RangeVar, ignoreInh bool) string {
	b := p.builder()

	if !node.Inh && !ignoreInh {
		b.keyword("ONLY")
	}

	schema := ""
	if node.Schemaname != "" {
		schema = p.identifier(node.Schemaname) + "."
	}

	b.append(schema + p.identifier(node.Relname))

	if node.Alias != nil {
		b.append(p.printAlias(node.Alias))
	}

	return b.join(" ")
}

func (p *printer) printAlias(node *nodes.Alias) string {
	if len(node.Colnames) > 0 {
		columns := p.printNodes(node.Colnames, ", ")

		return fmt.Sprintf("%s(%s)", node.Aliasname, columns)
	}

	return p.identifier(node.Aliasname)
}

func (p *printer) printParamRef(node *nodes.ParamRef) string {
	if node.Number == 0 {
		return "?"
	}

	return fmt.Sprintf("$%d", node.Number)
}

func (p *printer) printString(node *nodes.String) string {
	return p.identifier(node.Str)
}

func (p *printer) printAStar(_ *nodes.AStar) string {
	return "*"
}

func (p *printer) printRawStmt(node *nodes.RawStmt) string {
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

func (p *printer) printAConst(node *nodes.AConst) string {
	s, ok := node.Val.(*nodes.String)
	if ok {
		return quote(s.Str)
	}

	return p.printNode(node.Val)
}

func (p *printer) printInteger(node *nodes.Integer) string {
	return strconv.Itoa(int(node.Ival))
}

func (p *printer) printFloat(node *nodes.Float) string {
	return node.Str
}

func (p *printer) printBitString(node *nodes.BitString) string {
	return "B'" + node.Str[1:] + "'"
}

func (p *printer) printNull(_ *nodes.Null) string {
	return p.keyword("NULL")
}

func (p *printer) relPersistence(n *nodes.RangeVar) string {
	switch n.Relpersistence {
	case "t":
		return p.keyword("TEMP")
	case "u":
		return p.keyword("UNLOGGED")
	}

	return ""
}

func (p *printer) printCreateStmt(node *nodes.CreateStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.append(p.relPersistence(node.Relation))
	b.keyword("TABLE")

	if node.IfNotExists {
		b.keyword("IF NOT EXISTS")
	}

	name := p.printNode(node.Relation)

	if node.OfTypename != nil {
		name = name + p.keyword(" OF ") + p.identifier(p.printTypeName(node.OfTypename))
	}

	b.append(name)

	sub := p.printSubClause(node.TableElts)
	if sub == "" {
		// Empty table definitions are valid
		sub = "()"
	}

	b.addToLast(sub)

	if len(node.InhRelations) > 0 {
		b.LF()
		b.keyword("INHERITS")
		b.append(p.printSubClauseInline(node.InhRelations))
	}

	opts := p.printNodes(node.Options, ",")
	if opts != "" {
		b.LF()
		b.keyword("WITH")
		b.append(opts)
	}

	if node.Tablespacename != "" {
		b.LF()
		b.keyword("TABLESPACE")
		b.append(node.Tablespacename)
	}

	return b.join(" ")
}

func (p *printer) printDeleteStmt(node *nodes.DeleteStmt) string {
	b := p.builder()
	b.append(p.printNode(node.WithClause))
	b.keyword("DELETE FROM")
	b.append(p.printNode(node.Relation))
	b.LF()

	u := p.printNodes(node.UsingClause, ", ")
	if u != "" {
		b.keyword("USING")
		b.append(u)
		b.LF()
	}

	sub := p.printNode(node.WhereClause)
	if sub != "" {
		b.keyword("WHERE")
		b.LF()
		b.appendPadded(sub)
	}

	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
		b.keyword("RETURNING")
		b.append(r)
	}

	return b.join(" ")
}

func (p *printer) printColumnDef(node *nodes.ColumnDef) string {
	b := p.builder()

	b.identifier(node.Colname)
	b.append(p.printNode(node.TypeName))

	r := p.printNode(node.RawDefault)
	if r != "" {
		b.keyword("USING")
		b.append(r)
	}

	b.append(p.printNodes(node.Constraints, " "))

	if node.CollClause != nil {
		b.keyword("COLLATE")
		b.append(p.printArr(node.CollClause.Collname)...)
	}

	return b.join(" ")
}

const keywordInterval = "interval"

func (p *printer) printTypeName(node *nodes.TypeName) string {
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

	b.append(name)

	if name == keywordInterval && len(node.Typmods) > 0 {
		i := getInt32(node.Typmods[0])
		b.keyword(intervalModType(i).String())

		if len(node.Typmods) > 1 {
			// Precision
			i := getInt32(node.Typmods[1])
			b.append("(" + strconv.Itoa(int(i)) + ")")
		}
	}

	return b.join(" ")
}

func getInt32(node nodes.Node) int32 {
	val, ok := node.(*nodes.Integer)
	if ok {
		return val.Ival
	}

	aConst, ok := node.(*nodes.AConst)
	if ok {
		val, ok := aConst.Val.(*nodes.Integer)
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

	name = name[len("pg_catalog."):]
	switch name {
	case "bpchar":
		return typeWrapper("char", args)
	case "varchar":
		return typeWrapper("varchar", args)
	case "numeric":
		return typeWrapper("numeric", args)
	}

	return nodes.PgTypeNameToKeyword[name]
}

func (p *printer) printConstraint(node *nodes.Constraint) string {
	b := p.builder()

	if node.Conname != "" {
		b.keyword("CONSTRAINT")
		b.append(node.Conname)
	}

	if node.Contype == nodes.CONSTR_FOREIGN {
		if len(node.FkAttrs) > 1 {
			b.keyword("FOREIGN KEY")
		}
	} else {
		b.keyword(nodes.ConstrTypeKeyword[node.Contype])
	}

	pre := ""
	post := ""

	if node.Contype == nodes.CONSTR_CHECK {
		pre = "("
		post = ")"
	}

	b.append(pre + p.printNode(node.RawExpr) + post)
	b.append(p.printSubClauseInlineSpace(node.Keys))
	b.append(p.printSubClauseInlineSpace(node.FkAttrs))

	if node.Pktable != nil {
		b.keyword("REFERENCES")
		b.append(p.printNode(node.Pktable), p.printSubClauseInlineSpace(node.PkAttrs))
	}

	if node.SkipValidation {
		b.keyword("NOT VALID")
	}

	if node.Indexname != "" {
		b.keyword("USING INDEX")
		b.append(node.Indexname)
	}

	opts := p.printNodes(node.Options, ",")
	if opts != "" {
		b.keyword("WITH")
		b.append(opts)
	}

	if len(node.Exclusions) > 0 {
		p.printConstraintExclusions(node, &b)
	}

	return b.join(" ")
}

func (p *printer) printConstraintExclusions(node *nodes.Constraint, b *sqlBuilder) {
	b.keyword("USING")
	b.append(node.AccessMethod)

	for _, n := range node.Exclusions {
		nn, ok := n.(*nodes.List)
		if ok {
			b.append("(" + p.printNodes(nn.Items, " WITH ") + ")")
		}
	}
}

func parseBool(s string) bool {
	return s == "t" || s == "'t'"
}

func (p *printer) printTypeCast(node *nodes.TypeCast) string {
	a := p.printNode(node.Arg)

	t := p.printNode(node.TypeName)
	if t == "boolean" {
		return strconv.FormatBool(parseBool(a))
	}

	return a + "::" + t
}

func (p *printer) printList(node *nodes.List) string {
	return p.printNodes(node.Items, ", ")
}

func (p *printer) printFuncCall(node *nodes.FuncCall) string {
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

func (p *printer) printCreateSchemaStmt(node *nodes.CreateSchemaStmt) string {
	b := p.builder()
	b.keyword("CREATE SCHEMA")

	if node.IfNotExists {
		b.keyword("IF NOT EXISTS")
	}

	if node.Schemaname != "" {
		b.append(p.identifier(node.Schemaname))
	}

	if node.Authrole != nil {
		b.keyword("AUTHORIZATION")
		b.append(p.printNode(node.Authrole))
	}

	if len(node.SchemaElts) > 0 {
		b.append(p.printSpaced(node.SchemaElts))
	}

	return b.join(" ")
}

func (p *printer) printCaseExpr(node *nodes.CaseExpr) string {
	b := p.builder()
	b.keyword("CASE")
	b.append(p.printNode(node.Arg))
	whens := p.printSpaced(node.Args)
	b.append(whens)

	sub := p.printNode(node.Defresult)
	if sub != "" {
		b.appendPadded(p.keyword("ELSE ") + sub)
	}

	b.append("END")

	return b.join(" ")
}

func (p *printer) printAArrayExpr(node *nodes.AArrayExpr) string {
	return fmt.Sprintf("%s[%s]", p.keyword("ARRAY"), p.printNodes(node.Elements, ", "))
}

func (p *printer) printCaseWhen(node *nodes.CaseWhen) string {
	b := p.builder()
	b.keyword("WHEN")
	b.append(p.printNode(node.Expr))
	b.keyword("THEN")
	b.append(p.printNode(node.Result))

	return b.join(" ")
}

func (p *printer) printCoalesceExpr(node *nodes.CoalesceExpr) string {
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

func (p *printer) printCreateEnumStmt(node *nodes.CreateEnumStmt) string {
	b := p.builder()
	b.keyword("CREATE TYPE")
	b.append(p.printNodes(node.TypeName, "."))
	b.keyword("AS ENUM")

	var vals []string

	for _, n := range node.Vals {
		s, ok := n.(*nodes.String)
		if ok {
			vals = append(vals, s.Str)
		}
	}

	b.append("(" + strings.Join(quoted(vals), ", ") + ")")

	return b.join(" ")
}

func (p *printer) printCommentStmt(node *nodes.CommentStmt) string {
	b := p.builder()
	b.keyword("COMMENT ON")
	b.keyword(nodes.ObjectTypeKeyword[node.Objtype])

	switch n := node.Object.(type) {
	case *nodes.String:
		b.append(n.Str)
	case *nodes.TypeName:
		b.append(p.printTypeName(n))
	case *nodes.List:
		b.identifier(p.printArr(n.Items)...)
	}

	b.keyword("IS")
	b.append(quote(node.Comment))

	return b.join(" ")
}

func (p *printer) printSubClauseCustom(prefix, suffix, sep string, nodes nodes.Nodes, allowPretty bool) string {
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

func (p *printer) printSubClause(nodes nodes.Nodes) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, true)
}

func (p *printer) printCSV(nodes nodes.Nodes) string {
	return p.printSubClauseCustom("", "", ",", nodes, true)
}

func (p *printer) printSpaced(nodes nodes.Nodes) string {
	return p.printSubClauseCustom("", "", " ", nodes, true)
}

func (p *printer) printSubClauseInline(nodes nodes.Nodes) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, false)
}

func (p *printer) printSubClauseInlineSpace(nodes nodes.Nodes) string {
	return p.printSubClauseCustom("(", ")", ", ", nodes, false)
}

func (p *printer) printCompositeTypeStmt(node *nodes.CompositeTypeStmt) string {
	b := p.builder()
	b.keyword("CREATE TYPE")
	b.append(p.printRangeVarInternal(node.Typevar, true))
	b.keyword("AS")
	b.append(p.printSubClause(node.Coldeflist))

	return b.join(" ")
}

func (p *printer) printCommonTableExpr(node *nodes.CommonTableExpr) string {
	b := p.builder()
	b.append(p.identifier(node.Ctename) + p.printSubClauseInlineSpace(node.Aliascolnames))
	b.keyword("AS")
	b.append("(")
	b.LF()
	b.appendPadded(p.printNode(node.Ctequery))
	b.append(")")

	return b.join(" ")
}

func (p *printer) printAlterTableStmt(node *nodes.AlterTableStmt) string {
	b := p.builder()
	b.keyword("ALTER")

	switch node.Relkind {
	case nodes.OBJECT_TABLE:
		b.keyword("TABLE")
	case nodes.OBJECT_VIEW:
		b.keyword("VIEW")
	}

	b.keywordIf("IF EXISTS", node.MissingOk)
	b.append(p.printRangeVar(node.Relation))
	b.append(p.printCSV(node.Cmds))

	return b.join(" ")
}

func (p *printer) printAlterTableCmd(node *nodes.AlterTableCmd) string {
	b := p.builder()

	c := nodes.AlterTableCommand[node.Subtype]
	if c != "" {
		b.keyword(c)
	}

	b.keywordIf("IF EXISTS", node.MissingOk)
	b.append(p.identifier(node.Name))

	if node.Newowner != nil {
		b.append(node.Newowner.Rolename)
	}

	opt := nodes.AlterTableOption[node.Subtype]

	def := p.printNode(node.Def)
	if node.Subtype == nodes.AT_ColumnDefault && def == "" {
		opt = "DROP DEFAULT"
	}

	if opt != "" {
		b.keyword(opt)
	}

	b.append(def)

	if node.Behavior == nodes.DROP_CASCADE {
		b.keyword("CASCADE")
	}

	return b.join(" ")
}

func (p *printer) printRenameStmt(node *nodes.RenameStmt) string {
	b := p.builder()
	b.keyword("ALTER")

	switch node.RenameType {
	case nodes.OBJECT_TABCONSTRAINT, nodes.OBJECT_COLUMN:
		b.keyword("TABLE")
	default:
		b.keyword(nodes.ObjectTypeKeyword[node.RenameType])
	}

	switch node.RenameType {
	case nodes.OBJECT_CONVERSION, nodes.OBJECT_COLLATION, nodes.OBJECT_TYPE,
		nodes.OBJECT_DOMCONSTRAINT, nodes.OBJECT_AGGREGATE, nodes.OBJECT_FUNCTION:
		b.append(p.printNode(node.Object))
	case nodes.OBJECT_TABLE, nodes.OBJECT_TABCONSTRAINT, nodes.OBJECT_INDEX, nodes.OBJECT_MATVIEW,
		nodes.OBJECT_VIEW, nodes.OBJECT_COLUMN:
		b.append(p.printNode(node.Relation))
	case nodes.OBJECT_TABLESPACE, nodes.OBJECT_RULE, nodes.OBJECT_TRIGGER:
		b.append(node.Subname)
		b.keyword("ON")
		b.append(p.printRangeVar(node.Relation))
	}

	b.keyword("RENAME")

	switch node.RenameType {
	case nodes.OBJECT_TABCONSTRAINT, nodes.OBJECT_DOMCONSTRAINT:
		b.keyword("CONSTRAINT")
		b.append(node.Subname)
	case nodes.OBJECT_COLUMN:
		b.append(node.Subname)
	}

	b.keyword("TO")
	b.identifier(node.Newname)

	return b.join(" ")
}

func (p *printer) printAlterObjectSchemaStmt(node *nodes.AlterObjectSchemaStmt) string {
	b := p.builder()
	b.keyword("ALTER")
	b.keyword(nodes.ObjectTypeKeyword[node.ObjectType])
	b.append(p.printNode(node.Object))
	b.append(p.printNode(node.Relation))
	b.keyword("SET SCHEMA")
	b.append(p.identifier(node.Newschema))
	b.keywordIf("IF EXISTS", node.MissingOk)

	return b.join(" ")
}

func (p *printer) printAlterEnumStmt(node *nodes.AlterEnumStmt) string {
	b := p.builder()
	b.keyword("ALTER TYPE")
	b.append(p.printNodes(node.TypeName, "."))

	if node.OldVal != "" {
		b.keyword("RENAME VALUE")
		b.append(quote(node.OldVal))
		b.keyword("TO")
		b.append(quote(node.NewVal))

		return b.join(" ")
	}

	b.keyword("ADD VALUE")
	b.keywordIf("IF NOT EXISTS", node.SkipIfNewValExists)
	b.append(quote(node.NewVal))

	if node.NewValNeighbor != "" {
		b.keywordIfElse("AFTER", "BEFORE", node.NewValIsAfter)
		b.append(quote(node.NewValNeighbor))
	}

	return b.join(" ")
}

func (p *printer) printCreateFunctionStmt(node *nodes.CreateFunctionStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.keywordIf("OR REPLACE", node.Replace)
	b.keyword("FUNCTION")
	b.identifier(p.printArr(node.Funcname)...)

	args := p.printSubClauseInlineSpace(node.Parameters)
	if args == "" {
		args = "()"
	}

	b.addToLast(args)
	b.keyword("RETURNS")
	b.append(p.printNode(node.ReturnType))
	b.append(p.printNodes(node.Options, " "))

	return b.join(" ")
}

func (p *printer) printFunctionParameter(node *nodes.FunctionParameter) string {
	b := p.builder()
	b.identifier(node.Name)
	t := p.printNode(node.ArgType)

	d := p.printNode(node.Defexpr)
	if d != "" {
		t += "=" + d
	}

	b.append(t)

	return b.join(" ")
}

func (p *printer) printDefElem(node *nodes.DefElem) string {
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
	case "analyze", "verbose", "costs", "settings", "buffers", "wal", "timing", "summary", "user_catalog_table", "strict":
		return p.keyword(node.Defname)
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
	case "oids":
		if arg != "" {
			return p.keyword("oids=") + stripQuote(arg)
		}

		return p.keyword("oids")
	}

	return p.keyword(arg)
}

func (p *printer) printBinaryList(nn []nodes.Node, sep string, invert bool) string {
	list := nn[0].(*nodes.List)
	o := p.printArr(list.Items)
	left := o[0]
	right := p.identifier(o[1:]...)

	if invert {
		return right + " " + sep + " " + left
	}

	return left + " " + sep + " " + right
}

func (p *printer) printDropStmt(node *nodes.DropStmt) string {
	b := p.builder()
	b.keyword("DROP")
	b.keyword(nodes.ObjectTypeKeyword[node.RemoveType])
	b.keywordIf("CONCURRENTLY", node.Concurrent)
	b.keywordIf("IF EXISTS", node.MissingOk)

	switch node.RemoveType {
	case nodes.OBJECT_CAST:
		tt := node.Objects[0].(*nodes.List)
		b.append(p.printSubClauseCustom("(", ")", p.keyword(" AS "), tt.Items, false))
	case nodes.OBJECT_FUNCTION, nodes.OBJECT_AGGREGATE, nodes.OBJECT_SCHEMA, nodes.OBJECT_EXTENSION:
		b.append(p.printNodes(node.Objects, ","))
	case nodes.OBJECT_OPFAMILY, nodes.OBJECT_OPCLASS:
		b.append(p.printBinaryList(node.Objects, p.keyword("USING"), true))
	case nodes.OBJECT_TRIGGER, nodes.OBJECT_RULE, nodes.OBJECT_POLICY:
		b.append(p.printBinaryList(node.Objects, p.keyword("ON"), true))
	case nodes.OBJECT_TRANSFORM:
		b.keyword("FOR")
		b.append(p.printBinaryList(node.Objects, p.keyword("LANGUAGE"), false))
	default:
		b.append(p.printNodes(node.Objects, ", "))
	}

	b.keywordIf("CASCADE", node.Behavior == nodes.DROP_CASCADE)

	return b.join(" ")
}

func (p *printer) printObjectWithArgs(node *nodes.ObjectWithArgs) string {
	b := p.builder()
	b.identifier(p.printArr(node.Objname)...)

	if !node.ArgsUnspecified {
		b.append(p.printSubClauseInlineSpace(node.Objargs))
	}

	return b.join("")
}

func (p *printer) printInsertStmt(node *nodes.InsertStmt) string {
	b := p.builder()
	b.append(p.printNode(node.WithClause))
	b.keyword("INSERT INTO")
	b.append(p.printNode(node.Relation) + p.printSubClauseInlineSpace(node.Cols))
	b.append(p.printNode(node.SelectStmt))

	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
		b.keyword("RETURNING")
		b.append(r)
	}

	return b.join(" ")
}

func (p *printer) printNamedArgExpr(node *nodes.NamedArgExpr) string {
	b := p.builder()
	b.identifier(node.Name)
	b.keyword("=>")
	b.append(p.printNode(node.Arg))

	return b.join(" ")
}

func (p *printer) printSubLink(node *nodes.SubLink) string {
	sub := "(" + p.printNode(node.Subselect) + ")"

	switch node.SubLinkType {
	case nodes.ANY_SUBLINK:
		return p.printNode(node.Testexpr) + p.keyword(" IN ") + sub
	case nodes.ALL_SUBLINK:
		return p.printNode(node.Testexpr) + " " + p.printNodes(node.OperName, " ") + p.keyword(" ALL ") + sub
	case nodes.EXISTS_SUBLINK:
		return p.keyword("EXISTS") + sub
	default:
		return sub
	}
}

func (p *printer) printBoolExpr(node *nodes.BoolExpr) string {
	b := p.builder()

	for _, n := range node.Args {
		bExpr, ok := n.(*nodes.BoolExpr)
		if ok && ((node.Boolop == nodes.AND_EXPR && bExpr.Boolop == nodes.OR_EXPR) || node.Boolop == nodes.OR_EXPR) {
			b.append("(" + p.printNode(n) + ")")
		} else {
			b.append(p.printNode(n))
		}
	}

	op := p.keyword("AND ")
	if node.Boolop == nodes.OR_EXPR {
		op = p.keyword("OR ")
	}

	if p.pretty {
		op = "\n" + op
	} else {
		op = " " + op
	}

	return b.join(op)
}

func (p *printer) printUpdateTargets(list nodes.Nodes) string {
	var (
		multi *nodes.MultiAssignRef
		names []string
	)

	b := p.builder()

	for i := range list {
		n := list[i]

		node, ok := n.(*nodes.ResTarget)
		if ok {
			v, ok := node.Val.(*nodes.MultiAssignRef)
			if ok {
				multi = v

				names = append(names, p.identifier(node.Name))
			} else {
				b.append(p.identifier(node.Name) + " = " + p.printNode(node.Val))
			}
		}
	}

	if multi != nil {
		b.append("(" + strings.Join(names, ", ") + ")" + " = " + p.printNode(multi))
	}

	return b.join(", ")
}

func (p *printer) printUpdateStmt(node *nodes.UpdateStmt) string {
	b := p.builder()
	b.append(p.printNode(node.WithClause))
	b.keyword("UPDATE")
	b.append(p.printNode(node.Relation))
	b.LF()
	b.keyword("SET")
	b.LF()
	b.appendPadded(p.printUpdateTargets(node.TargetList))

	w := p.printNode(node.WhereClause)
	if w != "" {
		b.keyword("WHERE")
		b.LF()
		b.appendPadded(w)
	}

	r := p.printNodes(node.ReturningList, ", ")
	if r != "" {
		b.keyword("RETURNING")
		b.append(r)
	}

	return b.join(" ")
}

func (p *printer) printCreateTableAsStmt(node *nodes.CreateTableAsStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.append(p.relPersistence(node.Into.Rel))
	b.keyword(nodes.ObjectTypeKeyword[node.Relkind])
	b.keywordIf("IF NOT EXISTS", node.IfNotExists)
	b.identifier(p.printNode(node.Into))
	b.append(p.printSubClauseInlineSpace(node.Into.ColNames))
	b.LF()

	opts := p.printSubClauseInline(node.Into.Options)
	if opts != "" {
		b.keyword("WITH")
		b.append(opts)
		b.LF()
	}

	if node.Into.OnCommit > nodes.ONCOMMIT_PRESERVE_ROWS {
		b.keyword("ON COMMIT")

		switch node.Into.OnCommit {
		case nodes.ONCOMMIT_DELETE_ROWS:
			b.keyword("DELETE ROWS")
		case nodes.ONCOMMIT_DROP:
			b.keyword("DROP")
		}

		b.LF()
	}

	if node.Into.TableSpaceName != "" {
		b.keyword("TABLESPACE")
		b.append(node.Into.TableSpaceName)
		b.LF()
	}

	b.keyword("AS")
	b.LF()
	b.append(p.printNode(node.Query))

	if node.Into.SkipData {
		b.LF()
		b.keyword("WITH NO DATA")
	}

	return b.join(" ")
}

func (p *printer) printIntoClause(node *nodes.IntoClause) string {
	return p.printNode(node.Rel)
}

func (p *printer) printSortBy(node *nodes.SortBy) string {
	b := p.builder()
	b.append(p.printNode(node.Node))

	switch node.SortbyDir {
	case nodes.SORTBY_ASC:
		b.keyword("ASC")
	case nodes.SORTBY_DESC:
		b.keyword("DESC")
	}

	switch node.SortbyNulls {
	case nodes.SORTBY_NULLS_FIRST:
		b.keyword("NULLS FIRST")
	case nodes.SORTBY_NULLS_LAST:
		b.keyword("NULLS LAST")
	}

	return b.join(" ")
}

func (p *printer) printCreateExtensionStmt(node *nodes.CreateExtensionStmt) string {
	b := p.builder()
	b.keyword("CREATE EXTENSION")
	b.keywordIf("IF NOT EXISTS", node.IfNotExists)
	b.identifier(node.Extname)

	opts := p.printNodes(node.Options, " ")
	if opts != "" {
		b.LF()
		b.appendPadded(p.keyword("WITH ") + opts)
	}

	return b.join(" ")
}

func (p *printer) printRangeSubselect(node *nodes.RangeSubselect) string {
	b := p.builder()
	b.append("(" + p.printNode(node.Subquery) + ")")

	a := p.printNode(node.Alias)
	if a != "" {
		b.append(a)
	}

	return b.join(" ")
}

func (p *printer) printTruncateStmt(node *nodes.TruncateStmt) string {
	b := p.builder()
	b.keyword("TRUNCATE")
	b.append(p.printNodes(node.Relations, ", "))
	b.keywordIf("RESTART IDENTITY", node.RestartSeqs)
	b.keywordIf("CASCADE", node.Behavior == nodes.DROP_CASCADE)

	return b.join(" ")
}

func (p *printer) printMultiAssignRef(node *nodes.MultiAssignRef) string {
	return "(" + p.printNode(node.Source) + ")"
}

func (p *printer) printRowExpr(node *nodes.RowExpr) string {
	return p.printNodes(node.Args, ", ")
}

func (p *printer) printExplainStmt(node *nodes.ExplainStmt) string {
	b := p.builder()
	b.keyword("EXPLAIN")
	b.append(p.printSubClauseInline(node.Options))
	b.LF()
	b.append(p.printNode(node.Query))

	return b.join(" ")
}

func (p *printer) printNullTest(node *nodes.NullTest) string {
	b := p.builder()
	b.append(p.printNode(node.Xpr), p.printNode(node.Arg))
	b.keywordIfElse("IS NULL", "IS NOT NULL", node.Nulltesttype == nodes.IS_NULL)

	return b.join(" ")
}

func (p *printer) printViewStmt(node *nodes.ViewStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.keywordIf("OR REPLACE", node.Replace)
	b.append(p.printNodes(node.Options, " "))
	b.keyword("VIEW")
	b.append(p.printNode(node.View) + p.printSubClauseInlineSpace(node.Aliases))
	b.keyword("AS")
	b.LF()
	b.append(p.printNode(node.Query))

	switch node.WithCheckOption {
	case nodes.LOCAL_CHECK_OPTION:
		b.keyword("WITH LOCAL CHECK OPTION")
	case nodes.CASCADED_CHECK_OPTION:
		b.keyword("WITH CASCADED CHECK OPTION")
	}

	return b.join(" ")
}

func (p *printer) printSqlvalueFunction(node *nodes.SqlvalueFunction) string {
	return nodes.SQLValueFunctionOpName[node.Op]
}

func (p *printer) printIndexElem(node *nodes.IndexElem) string {
	return node.Name
}

func (p *printer) printCurrentOfExpr(node *nodes.CurrentOfExpr) string {
	return p.keyword("CURRENT OF ") + node.CursorName
}

func (p *printer) printRangeFunction(node *nodes.RangeFunction) string {
	b := p.builder()
	b.append(p.printNodes(node.Functions, ", "))
	b.keywordIf("WITH ORDINALITY", node.Ordinality)

	if node.Alias != nil {
		b.keyword("AS")
		b.append(p.printAlias(node.Alias))
	}

	b.append(p.printSubClauseInlineSpace(node.Coldeflist))

	return b.join(" ")
}

func (p *printer) printLockingClause(node *nodes.LockingClause) string {
	b := p.builder()
	b.keyword("FOR " + nodes.LockClauseStrengthKeyword[node.Strength])

	switch node.WaitPolicy {
	case nodes.LockWaitError:
		b.keyword("NOWAIT")
	}

	return b.join(" ")
}

func (p *printer) printLockStmt(node *nodes.LockStmt) string {
	b := p.builder()
	b.keyword("LOCK")
	b.append(p.printNodes(node.Relations, ", "))
	b.keyword(nodes.LockModeKeyword[nodes.LockMode(node.Mode)])
	b.keywordIf("NOWAIT", node.Nowait)

	return b.join(" ")
}

func (p *printer) printSetToDefault(_ *nodes.SetToDefault) string {
	return "DEFAULT"
}

func (p *printer) printCreateCastStmt(node *nodes.CreateCastStmt) string {
	b := p.builder()
	b.keyword("CREATE CAST")
	b.append("(" + p.printTypeName(node.Sourcetype) + " AS " + p.printTypeName(node.Targettype) + ")")

	switch node.Context {
	case nodes.COERCION_IMPLICIT:
		b.keyword("WITHOUT FUNCTION AS IMPLICIT")
	case nodes.COERCION_ASSIGNMENT:
		b.keyword("WITH FUNCTION")
		b.append(p.printNode(node.Func))
		b.keyword("AS ASSIGNMENT")
	case nodes.COERCION_EXPLICIT:
		b.keyword("WITH INOUT")
	}

	return b.join(" ")
}

func (p *printer) printCreateOpClassStmt(node *nodes.CreateOpClassStmt) string {
	b := p.builder()
	b.keyword("CREATE OPERATOR CLASS")
	b.append(p.printNodes(node.Opclassname, ", "))
	b.keywordIf("DEFAULT", node.IsDefault)
	b.keyword("FOR TYPE")
	b.append(p.printTypeName(node.Datatype))
	b.keyword("USING")
	b.append(node.Amname)
	b.keyword("AS")
	b.append(p.printCSV(node.Items))

	return b.join(" ")
}

func (p *printer) printCreateOpClassItem(node *nodes.CreateOpClassItem) string {
	b := p.builder()

	switch node.Itemtype {
	case nodes.OperatorItemType:
		b.keyword("OPERATOR")
	case nodes.FunctionItemType:
		b.keyword("FUNCTION")
	}

	b.append(strconv.Itoa(int(node.Number)))
	b.append(p.printObjectWithArgs(node.Name))

	return b.join(" ")
}

func (p *printer) printWindowDef(node *nodes.WindowDef) string {
	b := p.builder()

	if node.Name != "" {
		b.identifier(node.Name)
	}

	if len(node.PartitionClause) > 0 {
		if node.Name != "" {
			b.keyword("AS")
		}

		b.keyword("(PARTITION BY")
		b.append(p.printNodes(node.PartitionClause, ", "))

		if len(node.OrderClause) > 0 {
			b.keyword("ORDER BY")
			b.append(p.printNodes(node.OrderClause, ", "))
		}

		b.addToLast(")")
	} else if node.Name == "" {
		b.append("()")
	}

	return b.join(" ")
}

func (p *printer) printRoleSpec(node *nodes.RoleSpec) string {
	return node.Rolename
}

func (p *printer) printRuleStmt(node *nodes.RuleStmt) string {
	b := p.builder()
	b.keyword("CREATE RULE")
	b.identifier(node.Rulename)
	b.keyword("AS")
	b.keyword("ON")
	b.keyword(nodes.CmdTypeKeyword[node.Event])
	b.keyword("TO")
	b.identifier(p.printRangeVar(node.Relation))

	if node.Instead {
		b.keyword("DO INSTEAD")
		b.append(p.printNodes(node.Actions, ", "))
	} else {
		b.keyword("DO ALSO NOTIFY")
		b.append(p.printNodes(node.Actions, ", "))
	}

	return b.join(" ")
}

func (p *printer) printNotifyStmt(node *nodes.NotifyStmt) string {
	return node.Conditionname
}

func (p *printer) printCreateTransformStmt(node *nodes.CreateTransformStmt) string {
	b := p.builder()
	b.keyword("CREATE TRANSFORM FOR")
	b.identifier(p.printTypeName(node.TypeName))
	b.keyword("LANGUAGE")
	b.identifier(node.Lang)
	b.append("(")
	b.LF()
	b.keyword("FROM SQL WITH FUNCTION")
	b.append(p.printObjectWithArgs(node.Fromsql) + ", ")
	b.LF()
	b.keyword("TO SQL WITH FUNCTION")
	b.append(p.printObjectWithArgs(node.Tosql))
	b.LF()
	b.append(")")

	return b.join(" ")
}
