package pgtree

import "fmt"

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

func (e LockClauseStrength) Keyword() string {
	switch e {
	case LCS_NONE:
		return ""
	case LCS_FORKEYSHARE:
		return "KEY SHARE"
	case LCS_FORSHARE:
		return "SHARE"
	case LCS_FORNOKEYUPDATE:
		return "NO KEY UPDATE"
	case LCS_FORUPDATE:
		return "UPDATE"
	}

	return fmt.Sprintf("LockClauseStrength(%d)", e)
}

type LockMode int8

const (
	LOCKMODE_AccessShare          LockMode = 1
	LOCKMODE_RowShare             LockMode = 2
	LOCKMODE_RowExclusive         LockMode = 3
	LOCKMODE_ShareUpdateExclusive LockMode = 4
	LOCKMODE_Share                LockMode = 5
	LOCKMODE_ShareRowExclusive    LockMode = 6
	LOCKMODE_Exclusive            LockMode = 7
	LOCKMODE_AccessExclusive      LockMode = 8
)

func (e LockMode) Keyword() string {
	switch e {
	case LOCKMODE_AccessShare:
		return "IN ACCESS SHARE MODE"
	case LOCKMODE_RowShare:
		return "IN ROW SHARE MODE"
	case LOCKMODE_RowExclusive:
		return "IN ROW EXCLUSIVE MODE"
	case LOCKMODE_ShareUpdateExclusive:
		return "IN SHARE UPDATE EXCLUSIVE MODE"
	case LOCKMODE_Share:
		return "IN SHARE MODE"
	case LOCKMODE_ShareRowExclusive:
		return "IN SHARE ROW EXCLUSIVE MODE"
	case LOCKMODE_Exclusive:
		return "IN EXCLUSIVE MODE"
	case LOCKMODE_AccessExclusive:
		return "IN ACCESS EXCLUSIVE MODE"
	}

	return fmt.Sprintf("LockMode(%d)", e)
}

func (e CmdType) Keyword() string {
	switch e {
	case CMD_SELECT:
		return "SELECT"
	case CMD_UPDATE:
		return "UPDATE"
	case CMD_INSERT:
		return "INSERT"
	case CMD_DELETE:
		return "DELETE"
	}

	return fmt.Sprintf("CmdType(%d)", e)
}
