CREATE UNLOGGED TABLE IF NOT EXISTS complex_table
    (
        did INTEGER
            PRIMARY KEY,
        did2 INTEGER
            CHECK (did2 > 100),
        no_null_int INTEGER
            CONSTRAINT no_null NOT NULL,
        no_null_varchar VARCHAR(40) COLLATE "es_ES" NOT NULL,
        some_unique INT8
            UNIQUE,
        some_varchar VARCHAR(40),
        some_num NUMERIC,
        some_float FLOAT4,
        some_float8 FLOAT8,
        some_time TIME,
        some_timetz TIMETZ,
        some_timestamptz TIMESTAMPTZ,
        default_value VARCHAR(40) DEFAULT 'value',
        next_int INTEGER DEFAULT NEXTVAL('some_serial'),
        default_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        c CIRCLE,
        vector INT2[][],
        len INTERVAL HOUR TO MINUTE,

        UNIQUE (some_varchar) WITH (FILLFACTOR =70),
        CONSTRAINT unique_no_null_varchar
            UNIQUE (no_null_varchar),
        CONSTRAINT con1
            CHECK (no_null_int > 100 AND some_varchar <> ''),
        EXCLUDE USING gist (c WITH &&)
    )
INHERITS (parent_table)
WITH (FILLFACTOR = 70)
TABLESPACE diskvol1;
