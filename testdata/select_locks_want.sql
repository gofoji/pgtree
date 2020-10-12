SELECT PG_ADVISORY_LOCK(q.id)
  FROM
          (SELECT id FROM foo WHERE id > 12345 LIMIT 100) q;
SELECT *
  FROM
      mytable FOR UPDATE NOWAIT;
SELECT *
  FROM
      mytable FOR NO KEY UPDATE;
SELECT *
  FROM
      mytable FOR SHARE;
SELECT *
  FROM
      mytable FOR KEY SHARE;
