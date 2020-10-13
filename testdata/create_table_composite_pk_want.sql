CREATE TEMP TABLE composite_pk(
    code char(5),
    title varchar(40),
    interval_range interval HOUR TO MINUTE,
    CONSTRAINT code_title PRIMARY KEY (code, title)
);
