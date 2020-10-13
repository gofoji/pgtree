WITH foo AS (
    SELECT * FROM foo
) INSERT INTO bar(a, b, c) SELECT * FROM foo RETURNING *;
INSERT INTO bar(a, b, c) VALUES (1, 2, 3), (1, 2, 4);
WITH cc AS (
    INSERT INTO abc.def(a, b, c, d, e) VALUES (1, 1, $1, $2, false) RETURNING d
) INSERT INTO test(a, b, c, d) VALUES ($3, $3, $1, (SELECT e FROM cc)) RETURNING *;
