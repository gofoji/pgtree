WITH foo AS (
    SELECT * FROM foo
) INSERT INTO bar(a, b, c) SELECT * FROM foo RETURNING *;
INSERT INTO bar(a, b, c) VALUES (1, 2, 3), (1, 2, 4);
