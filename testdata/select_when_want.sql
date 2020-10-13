SELECT
    CASE x
        WHEN 1 THEN x
        WHEN 2 THEN y
        ELSE z
    END AS label,
    CASE
        WHEN x BETWEEN 0 AND 10 THEN 'low'
        WHEN x BETWEEN 11 AND 20 THEN 'high'
    END AS label2,
    COALESCE(x, foo, z, 1) AS never_null
FROM
    foo
WHERE
    a IN (1, 2)
    OR b ~~ '%@%'
    OR a NOT IN (1, 5)
    OR x LIKE '%a%'
    OR y NOT LIKE 'asdf%'
    OR y ILIKE 'asdf%'
    OR y NOT ILIKE 'aaaa'
    OR y SIMILAR TO '1222'
    OR z = ?;
