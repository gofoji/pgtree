ALTER TABLE IF EXISTS ONLY table_name
    ADD column_name INT,
    ADD column_name2 TEXT COLLATE "en_US",
    DROP IF EXISTS column_name3 CASCADE,
    DROP column_name4,
    ALTER column_name5 TYPE INT8 USING 100,
    ALTER column_name6 TYPE INT4,
    ALTER column_name6 SET DEFAULT 12,
    ALTER column_name7 DROP DEFAULT;
