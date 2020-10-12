EXPLAIN (ANALYZE,VERBOSE)
SELECT *
  FROM
      tenk1 t1,
      tenk2 t2
 WHERE
       t1.unique1 < 100
   AND t1.unique2 = t2.unique2;
EXPLAIN (COSTS)
SELECT *
  FROM
      foo
 WHERE
     i = 4;
EXPLAIN (BUFFERS)
SELECT *
  FROM
      foo
 WHERE
     i = 4;
EXPLAIN (TIMING)
SELECT *
  FROM
      foo
 WHERE
     i = 4;
EXPLAIN (FORMAT TEXT)
SELECT *
  FROM
      foo
 WHERE
     i = 4;
