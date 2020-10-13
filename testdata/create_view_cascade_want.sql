CREATE VIEW pg_comedies AS
SELECT
    *
FROM
    comedies
WHERE
    classification = 'PG'
WITH CASCADED CHECK OPTION;
CREATE VIEW pg_comedies AS
SELECT
    *
FROM
    comedies
WHERE
    classification = 'PG'
WITH LOCAL CHECK OPTION;
