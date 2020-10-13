CREATE RULE "_RETURN" AS ON SELECT TO t1 DO INSTEAD SELECT * FROM t2;

CREATE RULE notify_me AS ON UPDATE TO mytable DO ALSO NOTIFY mytable;

DROP RULE notify_me on mytable;