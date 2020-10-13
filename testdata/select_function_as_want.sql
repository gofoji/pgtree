SELECT
    *
INTO
    new_table
FROM
    getrows('String', l => 123) AS foo (id int, name text);
