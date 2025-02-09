package pgtree

import (
	nodes "github.com/pganalyze/pg_query_go/v6"
)

// ObjectTypeKeyword maps the ObjectType enum to the SQL name keyword.
var ObjectTypeKeyword = map[nodes.ObjectType]string{
	nodes.ObjectType_OBJECT_ACCESS_METHOD:   "ACCESS METHOD",
	nodes.ObjectType_OBJECT_AGGREGATE:       "AGGREGATE",
	nodes.ObjectType_OBJECT_AMOP:            "AMOP",
	nodes.ObjectType_OBJECT_AMPROC:          "AMPROC",
	nodes.ObjectType_OBJECT_ATTRIBUTE:       "ATTRIBUTE",
	nodes.ObjectType_OBJECT_CAST:            "CAST",
	nodes.ObjectType_OBJECT_COLUMN:          "COLUMN",
	nodes.ObjectType_OBJECT_COLLATION:       "COLLATION",
	nodes.ObjectType_OBJECT_CONVERSION:      "CONVERSION",
	nodes.ObjectType_OBJECT_DATABASE:        "DATABASE",
	nodes.ObjectType_OBJECT_DEFAULT:         "DEFAULT",
	nodes.ObjectType_OBJECT_DEFACL:          "DEFAULT ACL",
	nodes.ObjectType_OBJECT_DOMAIN:          "DOMAIN",
	nodes.ObjectType_OBJECT_DOMCONSTRAINT:   "DOMCONSTRAINT",
	nodes.ObjectType_OBJECT_EVENT_TRIGGER:   "EVENT TRIGGER",
	nodes.ObjectType_OBJECT_EXTENSION:       "EXTENSION",
	nodes.ObjectType_OBJECT_FDW:             "FOREIGN DATA WRAPPER",
	nodes.ObjectType_OBJECT_FOREIGN_SERVER:  "SERVER",
	nodes.ObjectType_OBJECT_FOREIGN_TABLE:   "FOREIGN TABLE",
	nodes.ObjectType_OBJECT_FUNCTION:        "FUNCTION",
	nodes.ObjectType_OBJECT_INDEX:           "INDEX",
	nodes.ObjectType_OBJECT_LANGUAGE:        "LANGUAGE",
	nodes.ObjectType_OBJECT_LARGEOBJECT:     "LARGEOBJECT",
	nodes.ObjectType_OBJECT_MATVIEW:         "MATERIALIZED VIEW",
	nodes.ObjectType_OBJECT_OPCLASS:         "OPERATOR CLASS",
	nodes.ObjectType_OBJECT_OPERATOR:        "OPERATOR",
	nodes.ObjectType_OBJECT_OPFAMILY:        "OPERATOR FAMILY",
	nodes.ObjectType_OBJECT_POLICY:          "POLICY",
	nodes.ObjectType_OBJECT_PROCEDURE:       "PROCEDURE",
	nodes.ObjectType_OBJECT_PUBLICATION:     "PUBLICATION",
	nodes.ObjectType_OBJECT_ROLE:            "ROLE",
	nodes.ObjectType_OBJECT_ROUTINE:         "ROUTINE",
	nodes.ObjectType_OBJECT_RULE:            "RULE",
	nodes.ObjectType_OBJECT_SCHEMA:          "SCHEMA",
	nodes.ObjectType_OBJECT_SEQUENCE:        "SEQUENCE",
	nodes.ObjectType_OBJECT_SUBSCRIPTION:    "SUBSCRIPTION",
	nodes.ObjectType_OBJECT_STATISTIC_EXT:   "STATISTIC",
	nodes.ObjectType_OBJECT_TABCONSTRAINT:   "TABLE CONSTRAINT",
	nodes.ObjectType_OBJECT_TABLE:           "TABLE",
	nodes.ObjectType_OBJECT_TABLESPACE:      "TABLESPACE",
	nodes.ObjectType_OBJECT_TRANSFORM:       "TRANSFORM",
	nodes.ObjectType_OBJECT_TRIGGER:         "TRIGGER",
	nodes.ObjectType_OBJECT_TSCONFIGURATION: "TEXT SEARCH CONFIGURATION",
	nodes.ObjectType_OBJECT_TSDICTIONARY:    "TEXT SEARCH DICTIONARY",
	nodes.ObjectType_OBJECT_TSPARSER:        "TEXT SEARCH PARSER",
	nodes.ObjectType_OBJECT_TSTEMPLATE:      "TEXT SEARCH TEMPLATE",
	nodes.ObjectType_OBJECT_TYPE:            "TYPE",
	nodes.ObjectType_OBJECT_USER_MAPPING:    "USER MAPPING",
	nodes.ObjectType_OBJECT_VIEW:            "VIEW",

	// nodes.ObjectType_OBJECT_PARAMETER_ACL         ObjectType = 28
	// nodes.ObjectType_OBJECT_PUBLICATION_NAMESPACE ObjectType = 32
	// nodes.ObjectType_OBJECT_PUBLICATION_REL: "OBJECT_PUBLICATION_REL",

}

// SetOpKeyword maps SetOperation enums to sql keyword.
var SetOpKeyword = map[nodes.SetOperation]string{
	nodes.SetOperation_SETOP_UNION:     "UNION",
	nodes.SetOperation_SETOP_INTERSECT: "INTERSECT",
	nodes.SetOperation_SETOP_EXCEPT:    "EXCEPT",
}

const keywordInterval = "interval"

// IntervalModType is the internal bit mask for specifying things like "day to minute" or "year".
type IntervalModType uint32

// Interval types.
const (
	Empty IntervalModType = 1 << iota
	Month
	Year
	Day
	Julian
	TZ
	DTZ
	DynTZ
	IgnoreDTF
	AMPM
	Hour
	Minute
	Second
	MilliSecond
	MicroSecond
	DoY
	DoW
	Units
	ADBC
	AGO
	ABSBefore
	ABSAfter
	ISODate
	ISOTime
	Week
	Decade
	Century
	Millennium
	DTZMod
)

func (i IntervalModType) String() string {
	switch i {
	case Month:
		return "month"
	case Year:
		return "year"
	case Day:
		return "day"
	case Hour:
		return "hour"
	case Minute:
		return "minute"
	case Second:
		return "second"
	case Year | Month:
		return "year to month"
	case Hour | Day:
		return "day to hour"
	case Day | Hour | Minute:
		return "day to minute"
	case Day | Hour | Minute | Second:
		return "day to second"
	case Hour | Minute:
		return "hour to minute"
	case Hour | Minute | Second:
		return "hour to second"
	case Minute | Second:
		return "minute to second"
	}

	return ""
}

// PgTypeNameToKeyword maps internal type names to sql names.
var PgTypeNameToKeyword = map[string]string{
	"bool":        "boolean",
	"int2":        "smallint",
	"int4":        "int",
	"int8":        "bigint",
	"real":        "real",
	"float4":      "real",
	"float8":      "double precision",
	"time":        "time",
	"timetz":      "time with time zone",
	"timestamp":   "timestamp",
	"timestamptz": "timestamp with time zone",
	"interval":    "interval",
}

// ConstrTypeKeyword maps ConstrType enums to sql keyword.
var ConstrTypeKeyword = map[nodes.ConstrType]string{
	nodes.ConstrType_CONSTR_NULL:      "NULL",
	nodes.ConstrType_CONSTR_NOTNULL:   "NOT NULL",
	nodes.ConstrType_CONSTR_DEFAULT:   "DEFAULT",
	nodes.ConstrType_CONSTR_CHECK:     "CHECK",
	nodes.ConstrType_CONSTR_PRIMARY:   "PRIMARY KEY",
	nodes.ConstrType_CONSTR_UNIQUE:    "UNIQUE",
	nodes.ConstrType_CONSTR_EXCLUSION: "EXCLUDE",
	nodes.ConstrType_CONSTR_FOREIGN:   "FOREIGN KEY",
	nodes.ConstrType_CONSTR_GENERATED: "GENERATED",
	nodes.ConstrType_CONSTR_IDENTITY:  "GENERATED",
}

// ConstraintGeneratedWhenToKeyword maps Constraint GeneratedWhen clauses to keywords.
var ConstraintGeneratedWhenToKeyword = map[string]string{
	"a": "ALWAYS",
	"d": "BY DEFAULT",
}

// AlterTableCommand maps AlterTableType enums to sql commands.
var AlterTableCommand = map[nodes.AlterTableType]string{
	nodes.AlterTableType_AT_AddColumn:                 "ADD",
	nodes.AlterTableType_AT_AddColumnToView:           "ALTER",
	nodes.AlterTableType_AT_ColumnDefault:             "ALTER",
	nodes.AlterTableType_AT_CookedColumnDefault:       "ALTER",
	nodes.AlterTableType_AT_DropNotNull:               "DROP",
	nodes.AlterTableType_AT_SetNotNull:                "ALTER",
	nodes.AlterTableType_AT_SetExpression:             "ALTER",
	nodes.AlterTableType_AT_DropExpression:            "DROP",
	nodes.AlterTableType_AT_CheckNotNull:              "ALTER",
	nodes.AlterTableType_AT_SetStatistics:             "ALTER",
	nodes.AlterTableType_AT_SetOptions:                "ALTER",
	nodes.AlterTableType_AT_ResetOptions:              "ALTER",
	nodes.AlterTableType_AT_SetStorage:                "ALTER",
	nodes.AlterTableType_AT_SetCompression:            "ALTER",
	nodes.AlterTableType_AT_DropColumn:                "DROP",
	nodes.AlterTableType_AT_AddIndex:                  "ADD INDEX",
	nodes.AlterTableType_AT_ReAddIndex:                "ALTER",
	nodes.AlterTableType_AT_AddConstraint:             "ADD",
	nodes.AlterTableType_AT_ReAddConstraint:           "ALTER",
	nodes.AlterTableType_AT_ReAddDomainConstraint:     "ALTER",
	nodes.AlterTableType_AT_AlterConstraint:           "ALTER CONSTRAINT",
	nodes.AlterTableType_AT_ValidateConstraint:        "VALIDATE CONSTRAINT",
	nodes.AlterTableType_AT_AddIndexConstraint:        "ALTER",
	nodes.AlterTableType_AT_DropConstraint:            "DROP CONSTRAINT",
	nodes.AlterTableType_AT_ReAddComment:              "ALTER",
	nodes.AlterTableType_AT_AlterColumnType:           "ALTER",
	nodes.AlterTableType_AT_AlterColumnGenericOptions: "ALTER",
	nodes.AlterTableType_AT_ChangeOwner:               "OWNER TO",
	nodes.AlterTableType_AT_ClusterOn:                 "ALTER",
	nodes.AlterTableType_AT_DropCluster:               "DROP",
	nodes.AlterTableType_AT_SetLogged:                 "ALTER",
	nodes.AlterTableType_AT_SetUnLogged:               "ALTER",
	nodes.AlterTableType_AT_DropOids:                  "DROP",
	nodes.AlterTableType_AT_SetAccessMethod:           "ALTER",
	nodes.AlterTableType_AT_SetTableSpace:             "ALTER",
	nodes.AlterTableType_AT_SetRelOptions:             "SET",
	nodes.AlterTableType_AT_ResetRelOptions:           "RESET",
	nodes.AlterTableType_AT_ReplaceRelOptions:         "ALTER",
	nodes.AlterTableType_AT_EnableTrig:                "ALTER",
	nodes.AlterTableType_AT_EnableAlwaysTrig:          "ALTER",
	nodes.AlterTableType_AT_EnableReplicaTrig:         "ALTER",
	nodes.AlterTableType_AT_DisableTrig:               "ALTER",
	nodes.AlterTableType_AT_EnableTrigAll:             "ALTER",
	nodes.AlterTableType_AT_DisableTrigAll:            "ALTER",
	nodes.AlterTableType_AT_EnableTrigUser:            "ALTER",
	nodes.AlterTableType_AT_DisableTrigUser:           "ALTER",
	nodes.AlterTableType_AT_EnableRule:                "ALTER",
	nodes.AlterTableType_AT_EnableAlwaysRule:          "ALTER",
	nodes.AlterTableType_AT_EnableReplicaRule:         "ALTER",
	nodes.AlterTableType_AT_DisableRule:               "ALTER",
	nodes.AlterTableType_AT_AddInherit:                "ALTER",
	nodes.AlterTableType_AT_DropInherit:               "DROP",
	nodes.AlterTableType_AT_AddOf:                     "ALTER",
	nodes.AlterTableType_AT_DropOf:                    "DROP",
	nodes.AlterTableType_AT_ReplicaIdentity:           "ALTER",
	nodes.AlterTableType_AT_EnableRowSecurity:         "ALTER",
	nodes.AlterTableType_AT_DisableRowSecurity:        "ALTER",
	nodes.AlterTableType_AT_ForceRowSecurity:          "ALTER",
	nodes.AlterTableType_AT_NoForceRowSecurity:        "ALTER",
	nodes.AlterTableType_AT_GenericOptions:            "ALTER",
	nodes.AlterTableType_AT_AttachPartition:           "ALTER",
	nodes.AlterTableType_AT_DetachPartition:           "ALTER",
	nodes.AlterTableType_AT_DetachPartitionFinalize:   "ALTER",
	nodes.AlterTableType_AT_AddIdentity:               "ALTER",
	nodes.AlterTableType_AT_SetIdentity:               "ALTER",
	nodes.AlterTableType_AT_DropIdentity:              "DROP",
	nodes.AlterTableType_AT_ReAddStatistics:           "ALTER",
	// TODO: Audit table commands
}

// AlterTableOption maps AlterTableType enums to sql command options.
var AlterTableOption = map[nodes.AlterTableType]string{
	nodes.AlterTableType_AT_ColumnDefault:             "SET DEFAULT",
	nodes.AlterTableType_AT_DropNotNull:               "DROP NOT NULL",
	nodes.AlterTableType_AT_SetNotNull:                "SET NOT NULL",
	nodes.AlterTableType_AT_SetStatistics:             "SET STATISTICS",
	nodes.AlterTableType_AT_SetOptions:                "SET",
	nodes.AlterTableType_AT_ResetOptions:              "RESET",
	nodes.AlterTableType_AT_SetStorage:                "SET STORAGE",
	nodes.AlterTableType_AT_AlterColumnType:           "TYPE",
	nodes.AlterTableType_AT_AlterColumnGenericOptions: "OPTIONS",
}

// SQLValueFunctionOpName maps SQLValueFunctionOp to sql standard function name.
var SQLValueFunctionOpName = map[nodes.SQLValueFunctionOp]string{
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_DATE:        "current_date",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_TIME:        "current_time",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_TIME_N:      "current_time",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP:   "current_timestamp",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP_N: "current_timestamp",
	nodes.SQLValueFunctionOp_SVFOP_LOCALTIME:           "localtime",
	nodes.SQLValueFunctionOp_SVFOP_LOCALTIME_N:         "localtime",
	nodes.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP:      "localtimestamp",
	nodes.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP_N:    "localtimestamp",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_ROLE:        "current_role",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_USER:        "current_user",
	nodes.SQLValueFunctionOp_SVFOP_USER:                "user",
	nodes.SQLValueFunctionOp_SVFOP_SESSION_USER:        "session_user",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_CATALOG:     "current_catalog",
	nodes.SQLValueFunctionOp_SVFOP_CURRENT_SCHEMA:      "current_schema",
}

// LockClauseStrengthKeyword maps LockClauseStrength enums to sql keyword.
var LockClauseStrengthKeyword = map[nodes.LockClauseStrength]string{
	nodes.LockClauseStrength_LCS_NONE:           "",
	nodes.LockClauseStrength_LCS_FORKEYSHARE:    "KEY SHARE",
	nodes.LockClauseStrength_LCS_FORSHARE:       "SHARE",
	nodes.LockClauseStrength_LCS_FORNOKEYUPDATE: "NO KEY UPDATE",
	nodes.LockClauseStrength_LCS_FORUPDATE:      "UPDATE",
}

// LockMode enumeration of lock modes.
type LockMode uint8

// LockModes.
const (
	LockModeAccessShare          LockMode = 1
	LockModeRowShare             LockMode = 2
	LockModeRowExclusive         LockMode = 3
	LockModeShareUpdateExclusive LockMode = 4
	LockModeShare                LockMode = 5
	LockModeShareRowExclusive    LockMode = 6
	LockModeExclusive            LockMode = 7
	LockModeAccessExclusive      LockMode = 8
)

// LockModeKeyword maps LockMode enums to sql keyword.
var LockModeKeyword = map[LockMode]string{
	LockModeAccessShare:          "IN ACCESS SHARE MODE",
	LockModeRowShare:             "IN ROW SHARE MODE",
	LockModeRowExclusive:         "IN ROW EXCLUSIVE MODE",
	LockModeShareUpdateExclusive: "IN SHARE UPDATE EXCLUSIVE MODE",
	LockModeShare:                "IN SHARE MODE",
	LockModeShareRowExclusive:    "IN SHARE ROW EXCLUSIVE MODE",
	LockModeExclusive:            "IN EXCLUSIVE MODE",
	LockModeAccessExclusive:      "IN ACCESS EXCLUSIVE MODE",
}

// CmdTypeKeyword maps CmdType enums to sql keyword.
var CmdTypeKeyword = map[nodes.CmdType]string{
	nodes.CmdType_CMD_SELECT: "SELECT",
	nodes.CmdType_CMD_UPDATE: "UPDATE",
	nodes.CmdType_CMD_INSERT: "INSERT",
	nodes.CmdType_CMD_DELETE: "DELETE",
}

// Defines the operator types used in Op Class definitions.
const (
	OperatorItemType = 1
	FunctionItemType = 2
)
