package pgtree

import (
	"fmt"
	"strconv"
	"strings"

	nodes "github.com/pganalyze/pg_query_go/v6"
)

func (p *printer) printJoinExpr(node *nodes.JoinExpr) string {
	b := p.builder()
	b.append(p.printNode(node.Larg))
	b.LF()

	switch node.Jointype {
	case nodes.JoinType_JOIN_INNER:
		if node.IsNatural {
			b.keyword("NATURAL")
		} else if node.Quals == nil && len(node.UsingClause) == 0 {
			b.keyword("CROSS")
		}
	case nodes.JoinType_JOIN_LEFT:
		b.keyword("LEFT")
	case nodes.JoinType_JOIN_FULL:
		b.keyword("FULL")
	case nodes.JoinType_JOIN_RIGHT:
		b.keyword("RIGHT")
	default:
		p.addError(ErrPrinter.Wrap("unhandled JoinType: " + node.Jointype.String()))
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
	if !p.Pretty {
		return p.printSelectStmtInternal(node)
	}

	r := p.printSelectStmtInternal(node)

	if len(r) > p.SimpleLen {
		return r
	}

	p.Pretty = false
	r = p.printSelectStmtInternal(node)
	p.Pretty = true

	return r
}

func (p *printer) printSelectStmtInternal(node *nodes.SelectStmt) string {
	b := p.builder()
	sub := p.printWithClause(node.WithClause)
	b.append(sub)

	if node.Op != nodes.SetOperation_SETOP_NONE {
		b.append(p.printSelectStmt(node.Larg))
		b.LF()
		b.keyword(SetOpKeyword[node.Op])
		b.keywordIf("ALL", node.All)
		b.LF()
		b.append(p.printSelectStmt(node.Rarg))
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

		if p.Pretty {
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
	if p.Pretty && p.OneResultColumnPerLine {
		sep = ",\n"
	}

	b.appendPadded(strings.Join(p.printArr(node.TargetList), sep))

	if node.IntoClause != nil {
		b.keyword("INTO")
		b.LF()
		b.appendPadded(p.printIntoClause(node.IntoClause))
	}
}

func (p *printer) printAExpr(node *nodes.A_Expr) string {
	left := p.printNode(node.Lexpr)
	right := p.printNode(node.Rexpr)
	op := p.printNodes(node.Name, " ")

	switch node.Kind {
	case nodes.A_Expr_Kind_AEXPR_OP:
		return fmt.Sprintf("%s %s %s", left, op, right)
	case nodes.A_Expr_Kind_AEXPR_OP_ANY:
		return fmt.Sprintf("%s %s ANY(%s)", left, op, right)
	case nodes.A_Expr_Kind_AEXPR_IN:
		if op == "=" {
			op = "IN"
		} else {
			op = "NOT IN"
		}

		return fmt.Sprintf("%s %s (%s)", left, p.keyword(op), right)
	case nodes.A_Expr_Kind_AEXPR_LIKE:
		if op == "~~" {
			op = "LIKE"
		} else {
			op = "NOT LIKE"
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword(op), right)
	case nodes.A_Expr_Kind_AEXPR_ILIKE:
		if op == "~~*" {
			op = "ILIKE"
		} else {
			op = "NOT ILIKE"
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword(op), right)
	case nodes.A_Expr_Kind_AEXPR_SIMILAR:
		fc, ok := node.Rexpr.Node.(*nodes.Node_FuncCall)
		if ok {
			name := p.printNodes(fc.FuncCall.Funcname, ".")
			if (name == "pg_catalog.similar_escape" || name == "pg_catalog.similar_to_escape") && len(fc.FuncCall.Args) == 1 {
				right = p.printNode(fc.FuncCall.Args[0])
			}
		}

		return fmt.Sprintf("%s %s %s", left, p.keyword("SIMILAR TO"), right)
	case nodes.A_Expr_Kind_AEXPR_BETWEEN:
		l := node.Rexpr.Node.(*nodes.Node_List)
		low := p.printNode(l.List.Items[0])
		high := p.printNode(l.List.Items[1])

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
	return p.identifier(node.Sval)
}

func (p *printer) printAStar(_ *nodes.A_Star) string {
	return "*"
}

func (p *printer) printRawStmt(node *nodes.RawStmt) string {
	term := ";"
	if p.Pretty {
		term = ";\n"
	}

	out := p.printNode(node.Stmt)
	if out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}

	return out + term
}

func (p *printer) printAConst(node *nodes.A_Const) string {
	if s, ok := node.Val.(*nodes.A_Const_Sval); ok {
		return quote(s.Sval.Sval)
	}

	if i, ok := node.Val.(*nodes.A_Const_Ival); ok {
		return strconv.Itoa(int(i.Ival.Ival))
	}

	if f, ok := node.Val.(*nodes.A_Const_Fval); ok {
		return f.Fval.Fval
	}

	if b, ok := node.Val.(*nodes.A_Const_Boolval); ok {
		return strconv.FormatBool(b.Boolval.Boolval)
	}

	if bs, ok := node.Val.(*nodes.A_Const_Bsval); ok {
		return "B'" + bs.Bsval.Bsval[1:] + "'"
	}

	if node.Isnull {
		return p.keyword("NULL")
	}

	p.addError(fmt.Errorf("unhandled A_Const type: %T", node.Val))
	return ""
}

func (p *printer) printInteger(node *nodes.Integer) string {
	return strconv.Itoa(int(node.Ival))
}

func (p *printer) printFloat(node *nodes.Float) string {
	return node.Fval
}

func (p *printer) printBitString(node *nodes.BitString) string {
	return "B'" + node.Bsval[1:] + "'"
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

	name := p.printRangeVar(node.Relation)

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

	if len(node.Options) > 0 {
		b.LF()
		b.keyword("WITH")
		b.append(p.printSubClause(node.Options))
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
	b.append(p.printWithClause(node.WithClause))
	b.keyword("DELETE FROM")
	b.append(p.printRangeVar(node.Relation))
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
	if node.TypeName != nil {
		b.append(p.printTypeName(node.TypeName))
	}

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
		b.keyword(IntervalModType(i).String())

		if len(node.Typmods) > 1 {
			// Precision
			i := getInt32(node.Typmods[1])
			b.append("(" + strconv.Itoa(int(i)) + ")")
		}
	}

	return b.join(" ")
}

func getInt32(node *nodes.Node) int32 {
	val, ok := node.Node.(*nodes.Node_Integer)
	if ok {
		return val.Integer.Ival
	}

	aConst, ok := node.Node.(*nodes.Node_AConst)
	if ok {
		val, ok := aConst.AConst.Val.(*nodes.A_Const_Ival)
		if ok {
			return val.Ival.Ival
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

	return PgTypeNameToKeyword[name]
}

func (p *printer) printConstraint(node *nodes.Constraint) string {
	b := p.builder()

	if node.Conname != "" {
		b.keyword("CONSTRAINT")
		b.append(node.Conname)
	}

	if node.Contype == nodes.ConstrType_CONSTR_FOREIGN {
		if len(node.FkAttrs) > 1 {
			b.keyword("FOREIGN KEY")
		}
	} else {
		b.keyword(ConstrTypeKeyword[node.Contype])
	}

	pre := ""
	post := ""

	switch node.Contype {
	case nodes.ConstrType_CONSTR_GENERATED:
		b.keyword(ConstraintGeneratedWhenToKeyword[node.GeneratedWhen])
		b.keyword("AS")

		pre = "("
		post = ") " + p.keyword("STORED")
	case nodes.ConstrType_CONSTR_IDENTITY:
		b.keyword(ConstraintGeneratedWhenToKeyword[node.GeneratedWhen])
		b.keyword("AS IDENTITY")
	case nodes.ConstrType_CONSTR_CHECK:
		pre = "("
		post = ")"
	}

	b.append(pre + p.printNode(node.RawExpr) + post)
	b.append(p.printSubClauseInlineSpace(node.Keys))
	b.append(p.printSubClauseInlineSpace(node.FkAttrs))

	if node.Pktable != nil {
		b.keyword("REFERENCES")
		b.append(p.printRangeVar(node.Pktable), p.printSubClauseInlineSpace(node.PkAttrs))
	}

	if node.SkipValidation {
		b.keyword("NOT VALID")
	}

	if node.Indexname != "" {
		b.keyword("USING INDEX")
		b.append(node.Indexname)
	}

	if len(node.Options) > 0 {
		p.printConstraintOptions(node, &b)
	}

	if len(node.Exclusions) > 0 {
		p.printConstraintExclusions(node, &b)
	}

	return b.join(" ")
}

func (p *printer) printConstraintOptions(node *nodes.Constraint, b *sqlBuilder) {
	if node.Contype == nodes.ConstrType_CONSTR_IDENTITY {
		b.append(p.printSubClauseCustom("(", ")", " ", node.Options, false))
	} else {
		b.keyword("WITH")
		b.append(p.printSubClauseInline(node.Options))
	}
}

func (p *printer) printConstraintExclusions(node *nodes.Constraint, b *sqlBuilder) {
	b.keyword("USING")
	b.append(node.AccessMethod)

	for _, n := range node.Exclusions {
		nn, ok := n.Node.(*nodes.Node_List)
		if ok {
			b.append("(" + p.printNodes(nn.List.Items, " WITH ") + ")")
		}
	}
}

func parseBool(s string) bool {
	return s == "t" || s == "'t'"
}

func (p *printer) printTypeCast(node *nodes.TypeCast) string {
	a := p.printNode(node.Arg)

	t := p.printTypeName(node.TypeName)
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
		result += p.keyword(" OVER ") + p.printWindowDef(node.Over)
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
		b.append(p.printRoleSpec(node.Authrole))
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

func (p *printer) printAArrayExpr(node *nodes.A_ArrayExpr) string {
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
		s, ok := n.Node.(*nodes.Node_String_)
		if ok {
			vals = append(vals, s.String_.Sval)
		}
	}

	b.append("(" + strings.Join(quoted(vals), ", ") + ")")

	return b.join(" ")
}

func (p *printer) printCommentStmt(node *nodes.CommentStmt) string {
	b := p.builder()
	b.keyword("COMMENT ON")
	b.keyword(ObjectTypeKeyword[node.Objtype])

	switch n := node.Object.Node.(type) {
	case *nodes.Node_String_:
		b.append(n.String_.Sval)
	case *nodes.Node_TypeName:
		b.append(p.printTypeName(n.TypeName))
	case *nodes.Node_List:
		b.identifier(p.printArr(n.List.Items)...)
	}

	b.keyword("IS")
	b.append(quote(node.Comment))

	return b.join(" ")
}

func (p *printer) printSubClauseCustom(prefix, suffix, sep string, nodes []*nodes.Node, allowPretty bool) string {
	if allowPretty && p.Pretty {
		prefix += "\n"
		suffix = "\n" + suffix
		sep += "\n"
	}

	sub := p.printNodes(nodes, sep)
	if sub == "" {
		return ""
	}

	if allowPretty && p.Pretty {
		sub = p.padLines(sub)
	}

	return prefix + sub + suffix
}

func (p *printer) printSubClause(nodes []*nodes.Node) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, true)
}

func (p *printer) printCSV(nodes []*nodes.Node) string {
	return p.printSubClauseCustom("", "", ",", nodes, true)
}

func (p *printer) printSpaced(nodes []*nodes.Node) string {
	return p.printSubClauseCustom("", "", " ", nodes, true)
}

func (p *printer) printSubClauseInline(nodes []*nodes.Node) string {
	return p.printSubClauseCustom("(", ")", ",", nodes, false)
}

func (p *printer) printSubClauseInlineSpace(nodes []*nodes.Node) string {
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

func (p *printer) closeStatement(statement string) string {
	if statement[len(statement)-1] == '\n' {
		statement = statement[:len(statement)-1]
	}

	term := ";"
	if p.Pretty {
		term = ";\n"
	}

	return statement + term
}

func (p *printer) printAlterTableStmt(node *nodes.AlterTableStmt) string {
	b := p.builder()
	b.keyword("ALTER")

	switch node.Objtype {
	case nodes.ObjectType_OBJECT_TABLE:
		b.keyword("TABLE")
	case nodes.ObjectType_OBJECT_VIEW:
		b.keyword("VIEW")
	default:
		p.addError(fmt.Errorf("unknown object type %d", node.Objtype))
	}

	b.keywordIf("IF EXISTS", node.MissingOk)
	b.append(p.printRangeVar(node.Relation))
	b.append(p.printCSV(node.Cmds))

	return b.join(" ")
}

func (p *printer) printAlterTableCmd(node *nodes.AlterTableCmd) string {
	b := p.builder()

	c := AlterTableCommand[node.Subtype]
	if c != "" {
		b.keyword(c)
	}

	b.keywordIf("IF EXISTS", node.MissingOk)
	b.append(p.identifier(node.Name))

	if node.Newowner != nil {
		b.append(node.Newowner.Rolename)
	}

	opt := AlterTableOption[node.Subtype]

	def := p.printNode(node.Def)
	if node.Subtype == nodes.AlterTableType_AT_ColumnDefault && def == "" {
		opt = "DROP DEFAULT"
	}

	if opt != "" {
		b.keyword(opt)
	}

	b.append(def)

	if node.Behavior == nodes.DropBehavior_DROP_CASCADE {
		b.keyword("CASCADE")
	}

	return b.join(" ")
}

func (p *printer) printRenameStmt(node *nodes.RenameStmt) string {
	b := p.builder()
	b.keyword("ALTER")

	switch node.RenameType {
	case nodes.ObjectType_OBJECT_TABCONSTRAINT, nodes.ObjectType_OBJECT_COLUMN:
		b.keyword("TABLE")
	default:
		b.keyword(ObjectTypeKeyword[node.RenameType])
	}

	switch node.RenameType {
	case nodes.ObjectType_OBJECT_CONVERSION, nodes.ObjectType_OBJECT_COLLATION, nodes.ObjectType_OBJECT_TYPE,
		nodes.ObjectType_OBJECT_DOMCONSTRAINT, nodes.ObjectType_OBJECT_AGGREGATE, nodes.ObjectType_OBJECT_FUNCTION:
		b.append(p.printNode(node.Object))
	case nodes.ObjectType_OBJECT_TABLE, nodes.ObjectType_OBJECT_TABCONSTRAINT, nodes.ObjectType_OBJECT_INDEX, nodes.ObjectType_OBJECT_MATVIEW,
		nodes.ObjectType_OBJECT_VIEW, nodes.ObjectType_OBJECT_COLUMN:
		b.append(p.printRangeVar(node.Relation))
	case nodes.ObjectType_OBJECT_TABLESPACE, nodes.ObjectType_OBJECT_RULE, nodes.ObjectType_OBJECT_TRIGGER:
		b.append(node.Subname)
		b.keyword("ON")
		b.append(p.printRangeVar(node.Relation))
	}

	b.keyword("RENAME")

	switch node.RenameType {
	case nodes.ObjectType_OBJECT_TABCONSTRAINT, nodes.ObjectType_OBJECT_DOMCONSTRAINT:
		b.keyword("CONSTRAINT")
		b.append(node.Subname)
	case nodes.ObjectType_OBJECT_COLUMN:
		b.append(node.Subname)
	}

	b.keyword("TO")
	b.identifier(node.Newname)

	return b.join(" ")
}

func (p *printer) printAlterObjectSchemaStmt(node *nodes.AlterObjectSchemaStmt) string {
	b := p.builder()
	b.keyword("ALTER")
	b.keyword(ObjectTypeKeyword[node.ObjectType])
	b.append(p.printNode(node.Object))
	b.append(p.printRangeVar(node.Relation))
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
	b.append(p.printTypeName(node.ReturnType))
	b.append(p.printNodes(node.Options, " "))

	return b.join(" ")
}

func (p *printer) printFunctionParameter(node *nodes.FunctionParameter) string {
	b := p.builder()
	b.identifier(node.Name)
	t := p.printTypeName(node.ArgType)

	d := p.printNode(node.Defexpr)
	if d != "" {
		t += "=" + d
	}

	b.append(t)

	return b.join(" ")
}

var StorageParametersNumeric = map[string]interface{}{
	"fillfactor":                            nil,
	"toast_tuple_target":                    nil,
	"parallel_workers":                      nil,
	"autovacuum_vacuum_threshold":           nil,
	"autovacuum_vacuum_scale_factor":        nil,
	"autovacuum_analyze_threshold":          nil,
	"autovacuum_analyze_scale_factor":       nil,
	"autovacuum_vacuum_cost_delay":          nil,
	"autovacuum_vacuum_cost_limit":          nil,
	"autovacuum_freeze_min_age":             nil,
	"autovacuum_freeze_max_age":             nil,
	"autovacuum_freeze_table_age":           nil,
	"autovacuum_multixact_freeze_min_age":   nil,
	"autovacuum_multixact_freeze_max_age":   nil,
	"autovacuum_multixact_freeze_table_age": nil,
	"log_autovacuum_min_duration":           nil,
}

var StorageParametersBool = map[string]interface{}{
	"autovacuum_enabled":   nil,
	"vacuum_index_cleanup": nil,
	"vacuum_truncate":      nil,
	"user_catalog_table":   nil,
}

func (p *printer) printDefElem(node *nodes.DefElem) string {
	arg := p.printNode(node.Arg)
	ns := ""

	if node.Defnamespace != "" {
		ns = node.Defnamespace + "."
	}

	// Storage Parameters
	if _, ok := StorageParametersNumeric[node.Defname]; ok {
		return p.keyword(ns+node.Defname+"=") + arg
	}

	if _, ok := StorageParametersBool[node.Defname]; ok {
		arg = stripQuote(arg)
		if arg != "" {
			arg = "=" + arg
		}

		return p.keyword(ns + node.Defname + arg)
	}

	switch node.Defname {
	case "as":
		wrapper := " "
		if p.Pretty {
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
	case "schema":
		return p.keyword("SCHEMA ") + p.identifier(arg)
	case "new_version":
		return p.keyword("VERSION ") + p.identifier(arg)
	case "old_version":
		return p.keyword("FROM ") + p.identifier(arg)
	case "start":
		return p.keyword("START WITH ") + arg
	case "increment":
		return p.keyword("INCREMENT BY ") + arg
	case "oids":
		if arg != "" {
			return p.keyword("oids=") + stripQuote(arg)
		}

		return p.keyword("oids")
	}

	return p.keyword(arg)
}

func (p *printer) printBinaryList(nn []*nodes.Node, sep string, invert bool) string {
	list := nn[0].Node.(*nodes.Node_List)
	o := p.printArr(list.List.Items)
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
	b.keyword(ObjectTypeKeyword[node.RemoveType])
	b.keywordIf("CONCURRENTLY", node.Concurrent)
	b.keywordIf("IF EXISTS", node.MissingOk)

	switch node.RemoveType {
	case nodes.ObjectType_OBJECT_CAST:
		tt := node.Objects[0].Node.(*nodes.Node_List)
		b.append(p.printSubClauseCustom("(", ")", p.keyword(" AS "), tt.List.Items, false))
	case nodes.ObjectType_OBJECT_FUNCTION, nodes.ObjectType_OBJECT_AGGREGATE, nodes.ObjectType_OBJECT_SCHEMA, nodes.ObjectType_OBJECT_EXTENSION:
		b.append(p.printNodes(node.Objects, ","))
	case nodes.ObjectType_OBJECT_OPFAMILY, nodes.ObjectType_OBJECT_OPCLASS:
		b.append(p.printBinaryList(node.Objects, p.keyword("USING"), true))
	case nodes.ObjectType_OBJECT_TRIGGER, nodes.ObjectType_OBJECT_RULE, nodes.ObjectType_OBJECT_POLICY:
		b.append(p.printBinaryList(node.Objects, p.keyword("ON"), true))
	case nodes.ObjectType_OBJECT_TRANSFORM:
		b.keyword("FOR")
		b.append(p.printBinaryList(node.Objects, p.keyword("LANGUAGE"), false))
	default:
		b.append(p.printNodes(node.Objects, ", "))
	}

	b.keywordIf("CASCADE", node.Behavior == nodes.DropBehavior_DROP_CASCADE)

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
	b.append(p.printWithClause(node.WithClause))
	b.keyword("INSERT INTO")
	b.append(p.printRangeVar(node.Relation) + p.printSubClauseInlineSpace(node.Cols))
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
	case nodes.SubLinkType_ANY_SUBLINK:
		return p.printNode(node.Testexpr) + p.keyword(" IN ") + sub
	case nodes.SubLinkType_ALL_SUBLINK:
		return p.printNode(node.Testexpr) + " " + p.printNodes(node.OperName, " ") + p.keyword(" ALL ") + sub
	case nodes.SubLinkType_EXISTS_SUBLINK:
		return p.keyword("EXISTS") + sub
	default:
		return sub
	}
}

func (p *printer) printBoolExpr(node *nodes.BoolExpr) string {
	b := p.builder()

	for _, n := range node.Args {
		nestedBool, ok := n.Node.(*nodes.Node_BoolExpr)
		if ok && nestedBool.BoolExpr.Boolop != nodes.BoolExprType_NOT_EXPR {
			b.append("(" + p.printNode(n) + ")")
		} else {
			b.append(p.printNode(n))
		}
	}

	op := p.keyword("AND ")
	if node.Boolop == nodes.BoolExprType_OR_EXPR {
		op = p.keyword("OR ")
	}

	if p.Pretty {
		op = "\n" + op
	} else {
		op = " " + op
	}

	return b.join(op)
}

func (p *printer) printUpdateTargets(list []*nodes.Node) string {
	var (
		multi *nodes.MultiAssignRef
		names []string
	)

	b := p.builder()

	for i := range list {
		n := list[i]

		node, ok := n.Node.(*nodes.Node_ResTarget)
		if ok {
			v, ok := node.ResTarget.Val.Node.(*nodes.Node_MultiAssignRef)
			if ok {
				multi = v.MultiAssignRef

				names = append(names, p.identifier(node.ResTarget.Name))
			} else {
				b.append(p.identifier(node.ResTarget.Name) + " = " + p.printNode(node.ResTarget.Val))
			}
		}
	}

	if multi != nil {
		b.append("(" + strings.Join(names, ", ") + ")" + " = " + p.printMultiAssignRef(multi))
	}

	return b.join(", ")
}

func (p *printer) printUpdateStmt(node *nodes.UpdateStmt) string {
	b := p.builder()
	b.append(p.printWithClause(node.WithClause))
	b.keyword("UPDATE")
	b.append(p.printRangeVar(node.Relation))
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
	b.keyword(ObjectTypeKeyword[node.Objtype])
	b.keywordIf("IF NOT EXISTS", node.IfNotExists)
	b.identifier(p.printIntoClause(node.Into))
	b.append(p.printSubClauseInlineSpace(node.Into.ColNames))
	b.LF()

	opts := p.printSubClauseInline(node.Into.Options)
	if opts != "" {
		b.keyword("WITH")
		b.append(opts)
		b.LF()
	}

	if node.Into.OnCommit > nodes.OnCommitAction_ONCOMMIT_PRESERVE_ROWS {
		b.keyword("ON COMMIT")

		switch node.Into.OnCommit {
		case nodes.OnCommitAction_ONCOMMIT_DELETE_ROWS:
			b.keyword("DELETE ROWS")
		case nodes.OnCommitAction_ONCOMMIT_DROP:
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
	return p.printRangeVar(node.Rel)
}

func (p *printer) printSortBy(node *nodes.SortBy) string {
	b := p.builder()
	b.append(p.printNode(node.Node))

	switch node.SortbyDir {
	case nodes.SortByDir_SORTBY_ASC:
		b.keyword("ASC")
	case nodes.SortByDir_SORTBY_DESC:
		b.keyword("DESC")
	}

	switch node.SortbyNulls {
	case nodes.SortByNulls_SORTBY_NULLS_FIRST:
		b.keyword("NULLS FIRST")
	case nodes.SortByNulls_SORTBY_NULLS_LAST:
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

	a := p.printAlias(node.Alias)
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
	b.keywordIf("CASCADE", node.Behavior == nodes.DropBehavior_DROP_CASCADE)

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
	b.keywordIfElse("IS NULL", "IS NOT NULL", node.Nulltesttype == nodes.NullTestType_IS_NULL)

	return b.join(" ")
}

func (p *printer) printViewStmt(node *nodes.ViewStmt) string {
	b := p.builder()
	b.keyword("CREATE")
	b.keywordIf("OR REPLACE", node.Replace)
	b.append(p.printNodes(node.Options, " "))
	b.keyword("VIEW")
	b.append(p.printRangeVar(node.View) + p.printSubClauseInlineSpace(node.Aliases))
	b.keyword("AS")
	b.LF()
	b.append(p.printNode(node.Query))

	switch node.WithCheckOption {
	case nodes.ViewCheckOption_LOCAL_CHECK_OPTION:
		b.keyword("WITH LOCAL CHECK OPTION")
	case nodes.ViewCheckOption_CASCADED_CHECK_OPTION:
		b.keyword("WITH CASCADED CHECK OPTION")
	}

	return b.join(" ")
}

func (p *printer) printSqlvalueFunction(node *nodes.SQLValueFunction) string {
	return SQLValueFunctionOpName[node.Op]
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
	b.keyword("FOR " + LockClauseStrengthKeyword[node.Strength])

	switch node.WaitPolicy {
	case nodes.LockWaitPolicy_LockWaitError:
		b.keyword("NOWAIT")
	}

	return b.join(" ")
}

func (p *printer) printLockStmt(node *nodes.LockStmt) string {
	b := p.builder()
	b.keyword("LOCK")
	b.append(p.printNodes(node.Relations, ", "))
	b.keyword(LockModeKeyword[LockMode(node.Mode)])
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
	case nodes.CoercionContext_COERCION_IMPLICIT:
		b.keyword("WITHOUT FUNCTION AS IMPLICIT")
	case nodes.CoercionContext_COERCION_ASSIGNMENT:
		b.keyword("WITH FUNCTION")
		b.append(p.printObjectWithArgs(node.Func))
		b.keyword("AS ASSIGNMENT")
	case nodes.CoercionContext_COERCION_EXPLICIT:
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
	case OperatorItemType:
		b.keyword("OPERATOR")
	case FunctionItemType:
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
	b.keyword(CmdTypeKeyword[node.Event])
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

func (p *printer) printBoolean(node *nodes.Boolean) string {
	return strconv.FormatBool(node.Boolval)
}
