SELECT
DISTINCT ON (bar)
    2.3 AS monkey,
    NULL,
    B'101',
    1,
    2::int,
    b.goo::text[],
    'aaa',
    current_database(),
    "CURRENT_USER"(),
    u.*,
    count(DISTINCT f1)
FROM
    unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f']) WITH ORDINALITY AS u
    LEFT JOIN b ON b.id = u.b
    RIGHT JOIN (SELECT a + b AS sum FROM other_schema.foo) f ON f.sum = u.sum
    FULL JOIN x ON x.y = b.y
    NATURAL JOIN z
    CROSS JOIN asdf
    LEFT JOIN (VALUES
        (1, 'one'),
        (2, 'two'),
        (3, 'three')) t(num, letter) USING (num)
WHERE
    fooey > ALL (SELECT * FROM foobar)
    OR (EXISTS(SELECT * FROM foo2)
    AND b.x IS NOT NULL
    AND b.y IS NULL)
    OR ((f.sum > 100
    OR f.sum = 20
    OR f.sum < 1)
    AND b.bool)
    OR ($1 <> f."XXX"
    AND test.foo = ANY($2::bigserial[]))
GROUP BY b.moo
HAVING b.foo > 100
ORDER BY b.order_field DESC NULLS LAST, fieldx ASC NULLS FIRST
LIMIT 12
OFFSET 111;
