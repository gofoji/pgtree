ALTER TABLE IF EXISTS ONLY table_name
    ADD column_name INT,
    ADD COLUMN column_name2 TEXT COLLATE "en_US",
    DROP IF EXISTS column_name3 CASCADE,
    DROP column_name4 RESTRICT,
    ALTER column_name5 SET DATA TYPE INT8 USING 100,
    ALTER column_name6 TYPE INT4,
    ALTER column_name6 SET DEFAULT 12,
    ALTER column_name7 DROP DEFAULT;

-- ALTER [ COLUMN ] column_name DROP DEFAULT
--     ALTER [ COLUMN ] column_name { SET | DROP } NOT NULL
-- ALTER [ COLUMN ] column_name SET STATISTICS integer
-- ALTER [ COLUMN ] column_name SET ( attribute_option = value [, ... ] )
-- ALTER [ COLUMN ] column_name RESET ( attribute_option [, ... ] )
-- ALTER [ COLUMN ] column_name SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
--     ADD table_constraint [ NOT VALID ]
--     ADD table_constraint_using_index
-- ALTER CONSTRAINT constraint_name [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]
--     VALIDATE CONSTRAINT constraint_name
--     DROP CONSTRAINT [ IF EXISTS ]  constraint_name [ RESTRICT | CASCADE ]
--     DISABLE TRIGGER [ trigger_name | ALL | USER ]
--     ENABLE TRIGGER [ trigger_name | ALL | USER ]
--     ENABLE REPLICA TRIGGER trigger_name
--     ENABLE ALWAYS TRIGGER trigger_name
--     DISABLE RULE rewrite_rule_name
--     ENABLE RULE rewrite_rule_name
--     ENABLE REPLICA RULE rewrite_rule_name
--     ENABLE ALWAYS RULE rewrite_rule_name
-- CLUSTER ON index_name
-- SET WITHOUT CLUSTER
-- SET WITH OIDS
-- SET WITHOUT OIDS
-- SET ( storage_parameter = value [, ... ] )
-- RESET ( storage_parameter [, ... ] )
--     INHERIT parent_table
--     NO INHERIT parent_table
--     OF type_name
--     NOT OF
--     OWNER TO new_owner
-- SET TABLESPACE new_tablespace
--     REPLICA IDENTITY {DEFAULT | USING INDEX index_name | FULL | NOTHING}
--
--
-- ALTER TABLE [ IF EXISTS ] [ ONLY ] name [ * ]
--     RENAME [ COLUMN ] column_name TO new_column_name
--
-- ALTER TABLE [ IF EXISTS ] [ ONLY ] name [ * ]
--     RENAME CONSTRAINT constraint_name TO new_constraint_name
--
-- ALTER TABLE [ IF EXISTS ] name
--     RENAME TO new_name
--
-- ALTER TABLE [ IF EXISTS ] name
-- SET SCHEMA new_schema
--
--
-- ALTER TABLE ALL IN TABLESPACE name [ OWNED BY role_name [, ... ] ]
-- SET TABLESPACE new_tablespace [ NOWAIT ]