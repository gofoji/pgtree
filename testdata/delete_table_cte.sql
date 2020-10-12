  WITH
      table_b AS (SELECT id, name FROM table_x WHERE id > 100)
DELETE
  FROM
      table_a
 WHERE
     b_id IN (SELECT id FROM table_b WHERE name = 'foo');
