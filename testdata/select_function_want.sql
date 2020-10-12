SELECT *, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP,
       LOCALTIME, LOCALTIMESTAMP, TRANSACTION_TIMESTAMP(), STATEMENT_TIMESTAMP(), CLOCK_TIMESTAMP(), TIMEOFDAY(), NOW(),
       CURRENT_CATALOG, CURRENT_DATABASE(),
       CURRENT_QUERY(), CURRENT_ROLE, CURRENT_SCHEMA, current_schemas(TRUE), CURRENT_USER, inet_client_addr(), inet_client_port(), inet_server_addr(), inet_server_port(), pg_backend_pid(), pg_conf_load_time(), pg_is_other_temp_schema(OID), pg_listening_channels(), pg_my_temp_schema(), pg_postmaster_start_time(), pg_trigger_depth(), SESSION_USER, USER, VERSION ()
  FROM
      unnest(
      ARRAY [
      'a', 'b', 'c', 'd', 'e', 'f'])
  WITH
      ordinality;
