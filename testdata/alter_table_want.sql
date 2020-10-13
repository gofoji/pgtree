ALTER TABLE IF EXISTS ONLY table_name
    ADD column_name int,
    ADD column_name2 text COLLATE "en_US",
    DROP IF EXISTS column_name3 CASCADE,
    DROP column_name4,
    ALTER column_name5 TYPE int8 USING 100,
    ALTER column_name6 TYPE int4,
    ALTER column_name7 TYPE float8,
    ALTER column_name8 TYPE timetz,
    ALTER column_name9 TYPE timestamptz,
    ALTER column_name10 SET DEFAULT 12,
    ALTER column_name11 DROP DEFAULT
;
ALTER TABLE foo RENAME TO bar;
ALTER TABLE foo RENAME bar TO bar2;
ALTER TABLE foobar RENAME CONSTRAINT con_1 TO con_2;
ALTER TABLE fooey SET SCHEMA new_schema;
ALTER TABLE a
    ADD b int NULL
;
