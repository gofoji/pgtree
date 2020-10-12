CREATE TEMPORARY TABLE composite_pk
    (
        code CHAR(5),
        title VARCHAR(40),
        interval_range INTERVAL HOUR TO MINUTE,
        CONSTRAINT code_title
            PRIMARY KEY (code, title)
    );
