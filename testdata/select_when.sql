SELECT
    CASE x WHEN 1 THEN x WHEN 2 THEN y ELSE z END AS label,
    CASE WHEN x BETWEEN 0 AND 10 THEN 'low' WHEN x BETWEEN 11 AND 20 THEN 'high' END AS label2,
    COALESCE(x, foo, z, 1) AS never_null
  FROM
      foo
 WHERE
      a IN (1, 2)
   OR b ~~ '%@%'
OR a not in (1,5)
oR x like '%a%'
Or y not like 'asdf%'
or y ilike 'asdf%'
or y not ilike 'aaaa'
or y SIMILAR TO '1222'
or z = $1;