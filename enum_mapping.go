package pgtree

var objectTypeTypeName = map[ObjectType]string{
	OBJECT_ACCESS_METHOD:   "ACCESS METHOD",
	OBJECT_AGGREGATE:       "AGGREGATE",
	OBJECT_AMOP:            "AMOP",
	OBJECT_AMPROC:          "AMPROC",
	OBJECT_ATTRIBUTE:       "ATTRIBUTE",
	OBJECT_CAST:            "CAST",
	OBJECT_COLUMN:          "COLUMN",
	OBJECT_COLLATION:       "COLLATION",
	OBJECT_CONVERSION:      "CONVERSION",
	OBJECT_DATABASE:        "DATABASE",
	OBJECT_DEFAULT:         "DEFAULT",
	OBJECT_DEFACL:          "DEFAULT ACL",
	OBJECT_DOMAIN:          "DOMAIN",
	OBJECT_DOMCONSTRAINT:   "DOMCONSTRAINT",
	OBJECT_EVENT_TRIGGER:   "EVENT TRIGGER",
	OBJECT_EXTENSION:       "EXTENSION",
	OBJECT_FDW:             "FOREIGN DATA WRAPPER",
	OBJECT_FOREIGN_SERVER:  "SERVER",
	OBJECT_FOREIGN_TABLE:   "FOREIGN TABLE",
	OBJECT_FUNCTION:        "FUNCTION",
	OBJECT_INDEX:           "INDEX",
	OBJECT_LANGUAGE:        "LANGUAGE",
	OBJECT_LARGEOBJECT:     "LARGEOBJECT",
	OBJECT_MATVIEW:         "MATERIALIZED VIEW",
	OBJECT_OPCLASS:         "OPERATOR CLASS",
	OBJECT_OPERATOR:        "OPERATOR",
	OBJECT_OPFAMILY:        "OPERATOR FAMILY",
	OBJECT_POLICY:          "POLICY",
	OBJECT_PROCEDURE:       "PROCEDURE",
	OBJECT_PUBLICATION:     "PUBLICATION",
	OBJECT_PUBLICATION_REL: "OBJECT_PUBLICATION_REL",
	OBJECT_ROLE:            "ROLE",
	OBJECT_ROUTINE:         "ROUTINE",
	OBJECT_RULE:            "RULE",
	OBJECT_SCHEMA:          "SCHEMA",
	OBJECT_SEQUENCE:        "SEQUENCE",
	OBJECT_SUBSCRIPTION:    "SUBSCRIPTION",
	OBJECT_STATISTIC_EXT:   "STATISTIC",
	OBJECT_TABCONSTRAINT:   "TABLE CONSTRAINT",
	OBJECT_TABLE:           "TABLE",
	OBJECT_TABLESPACE:      "TABLESPACE",
	OBJECT_TRANSFORM:       "TRANSFORM",
	OBJECT_TRIGGER:         "TRIGGER",
	OBJECT_TSCONFIGURATION: "TEXT SEARCH CONFIGURATION",
	OBJECT_TSDICTIONARY:    "TEXT SEARCH DICTIONARY",
	OBJECT_TSPARSER:        "TEXT SEARCH PARSER",
	OBJECT_TSTEMPLATE:      "TEXT SEARCH TEMPLATE",
	OBJECT_TYPE:            "TYPE",
	OBJECT_USER_MAPPING:    "USER MAPPING",
	OBJECT_VIEW:            "VIEW",
}

var sqlValueFunctionOpLabel = map[SQLValueFunctionOp]string{
	SVFOP_CURRENT_DATE:        "current_date",
	SVFOP_CURRENT_TIME:        "current_time",
	SVFOP_CURRENT_TIME_N:      "current_time",
	SVFOP_CURRENT_TIMESTAMP:   "current_timestamp",
	SVFOP_CURRENT_TIMESTAMP_N: "current_timestamp",
	SVFOP_LOCALTIME:           "localtime",
	SVFOP_LOCALTIME_N:         "localtime",
	SVFOP_LOCALTIMESTAMP:      "localtimestamp",
	SVFOP_LOCALTIMESTAMP_N:    "localtimestamp",
	SVFOP_CURRENT_ROLE:        "current_role",
	SVFOP_CURRENT_USER:        "current_user",
	SVFOP_USER:                "user",
	SVFOP_SESSION_USER:        "session_user",
	SVFOP_CURRENT_CATALOG:     "current_catalog",
	SVFOP_CURRENT_SCHEMA:      "current_schema",
}

var lockClauseStrengthKeyword = map[LockClauseStrength]string{
	LCS_NONE:           "",
	LCS_FORKEYSHARE:    "KEY SHARE",
	LCS_FORSHARE:       "SHARE",
	LCS_FORNOKEYUPDATE: "NO KEY UPDATE",
	LCS_FORUPDATE:      "UPDATE",
}

type lockMode int8

// LockModes.
const (
	LockModeAccessShare          lockMode = 1
	LockModeRowShare             lockMode = 2
	LockModeRowExclusive         lockMode = 3
	LockModeShareUpdateExclusive lockMode = 4
	LockModeShare                lockMode = 5
	LockModeShareRowExclusive    lockMode = 6
	LockModeExclusive            lockMode = 7
	LockModeAccessExclusive      lockMode = 8
)

var lockModeKeyword = map[lockMode]string{
	LockModeAccessShare:          "IN ACCESS SHARE MODE",
	LockModeRowShare:             "IN ROW SHARE MODE",
	LockModeRowExclusive:         "IN ROW EXCLUSIVE MODE",
	LockModeShareUpdateExclusive: "IN SHARE UPDATE EXCLUSIVE MODE",
	LockModeShare:                "IN SHARE MODE",
	LockModeShareRowExclusive:    "IN SHARE ROW EXCLUSIVE MODE",
	LockModeExclusive:            "IN EXCLUSIVE MODE",
	LockModeAccessExclusive:      "IN ACCESS EXCLUSIVE MODE",
}

var cmdTypeKeyword = map[CmdType]string{
	CMD_SELECT: "SELECT",
	CMD_UPDATE: "UPDATE",
	CMD_INSERT: "INSERT",
	CMD_DELETE: "DELETE",
}

var setOpUnionKeyword = map[SetOperation]string{
	SETOP_UNION:     "UNION",
	SETOP_INTERSECT: "INTERSECT",
	SETOP_EXCEPT:    "EXCEPT",
}

var constrTypeKeyword = map[ConstrType]string{
	CONSTR_NULL:      "NULL",
	CONSTR_NOTNULL:   "NOT NULL",
	CONSTR_DEFAULT:   "DEFAULT",
	CONSTR_CHECK:     "CHECK",
	CONSTR_PRIMARY:   "PRIMARY KEY",
	CONSTR_UNIQUE:    "UNIQUE",
	CONSTR_EXCLUSION: "EXCLUDE",
	CONSTR_FOREIGN:   "FOREIGN KEY",
}

type commandOption struct {
	command string
	option  string
}

var alterTableCommand = map[AlterTableType]commandOption{
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

const (
	operatorItemType = 1
	functionItemType = 2
)

var pgTypeNameToKeyword = map[string]string{
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
