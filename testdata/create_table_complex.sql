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
        vector INT2[3][],
        len INTERVAL HOUR TO MINUTE,

        UNIQUE (some_varchar) WITH (FILLFACTOR =70),
        CONSTRAINT unique_no_null_varchar
            UNIQUE (no_null_varchar),
        CONSTRAINT con1
            CHECK (no_null_int > 100 AND some_varchar <> ''),
        EXCLUDE USING gist (c WITH &&),
        FOREIGN KEY (b, c) REFERENCES other_table (c1, c2) NOT VALID,
        CONSTRAINT unique_package_id UNIQUE USING INDEX package_tmp_id_idx
    )
INHERITS (parent_table)
WITH (
    fillfactor=1,
    toast_tuple_target=2,
    parallel_workers=3,
    autovacuum_vacuum_threshold=4,
    toast.autovacuum_vacuum_threshold=5,
    autovacuum_vacuum_scale_factor=6,
    toast.autovacuum_vacuum_scale_factor=7,
    autovacuum_analyze_threshold=8,
    autovacuum_analyze_scale_factor=9,
    autovacuum_vacuum_cost_delay =10,
    toast.autovacuum_vacuum_cost_delay=11,
    autovacuum_vacuum_cost_limit =12,
    toast.autovacuum_vacuum_cost_limit=13,
    autovacuum_freeze_min_age =14,
    toast.autovacuum_freeze_min_age=15,
    autovacuum_freeze_max_age=16,
    toast.autovacuum_freeze_max_age=17,
    autovacuum_freeze_table_age=18,
    toast.autovacuum_freeze_table_age=19,
    autovacuum_multixact_freeze_min_age=20,
    toast.autovacuum_multixact_freeze_min_age=21,
    autovacuum_multixact_freeze_max_age=22,
    toast.autovacuum_multixact_freeze_max_age=23,
    autovacuum_multixact_freeze_table_age=24,
    toast.autovacuum_multixact_freeze_table_age=25,
    log_autovacuum_min_duration=26,
    toast.log_autovacuum_min_duration=27,
    autovacuum_enabled=true,
    toast.autovacuum_enabled=true,
    vacuum_index_cleanup=true,
    toast.vacuum_index_cleanup=true,
    vacuum_truncate=false,
    toast.vacuum_truncate=false,
    user_catalog_table=false)
TABLESPACE diskvol1;
