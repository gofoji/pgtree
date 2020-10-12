DELETE
  FROM
      table_a USING table_b
 WHERE
       b_id = table_b.id
   AND table_b.name = 'foo'
RETURNING *;
