ALTER TABLE IF EXISTS ONLY table_name
    ADD column_name int,
    ADD column_name2 text COLLATE "en_US",
    DROP IF EXISTS column_name3 CASCADE,
    DROP column_name4,
    ALTER column_name5 TYPE int8 USING 100,
    ALTER column_name6 TYPE int4,
    ALTER column_name6 SET DEFAULT 12,
    ALTER column_name7 DROP DEFAULT
;
ALTER TABLE foo RENAME TO bar;
ALTER TABLE foo RENAME bar TO bar2;
ALTER TABLE foobar RENAME CONSTRAINT con_1 TO con_2;
ALTER TABLE fooey SET SCHEMA new_schema;
