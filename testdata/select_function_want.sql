SELECT
    *,
    count(*),
    current_date,
    current_time,
    current_timestamp,
    current_time,
    current_timestamp,
    localtime,
    localtimestamp,
    localtime,
    localtimestamp,
    transaction_timestamp(),
    statement_timestamp(),
    clock_timestamp(),
    timeofday(),
    now(),
    current_catalog,
    current_database(),
    current_query(),
    current_role,
    current_schema,
    current_schemas(true),
    current_user,
    inet_client_addr(),
    inet_client_port(),
    inet_server_addr(),
    inet_server_port(),
    pg_backend_pid(),
    pg_conf_load_time(),
    pg_is_other_temp_schema(oid),
    pg_listening_channels(),
    pg_my_temp_schema(),
    pg_postmaster_start_time(),
    pg_trigger_depth(),
    session_user,
    user,
    version()
FROM
    unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f']) WITH ORDINALITY;
