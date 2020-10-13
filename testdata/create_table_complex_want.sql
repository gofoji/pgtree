CREATE UNLOGGED TABLE IF NOT EXISTS complex_table(
    did int PRIMARY KEY,
    did2 int CHECK (did2 > 100),
    no_null_int int CONSTRAINT no_null NOT NULL,
    no_null_varchar varchar(40) NOT NULL COLLATE "es_ES",
    some_unique int8 UNIQUE,
    some_varchar varchar(40),
    some_num numeric,
    some_float float4,
    some_float8 float8,
    some_time time,
    some_timetz timetz,
    some_timestamptz timestamptz,
    default_value varchar(40) DEFAULT 'value',
    next_int int DEFAULT nextval('some_serial'),
    default_time timestamp DEFAULT current_timestamp,
    c circle,
    vector int2[3][],
    len interval HOUR TO MINUTE,
    UNIQUE (some_varchar) WITH (FILLFACTOR=70),
    CONSTRAINT unique_no_null_varchar UNIQUE (no_null_varchar),
    CONSTRAINT con1 CHECK (no_null_int > 100
    AND some_varchar <> ''),
    EXCLUDE USING gist (c WITH &&),
    FOREIGN KEY (b, c) REFERENCES other_table (c1, c2) NOT VALID,
    CONSTRAINT unique_package_id UNIQUE USING INDEX package_tmp_id_idx
)
INHERITS (parent_table)
WITH (FILLFACTOR=70)
TABLESPACE diskvol1;
