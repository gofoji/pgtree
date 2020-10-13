UPDATE ONLY weather
SET
    (temp_lo, temp_hi, prcp) = (temp_lo + 1, temp_lo + 15, DEFAULT)
WHERE
    city = 'San Francisco'
    AND date = '2003-07-03'
RETURNING temp_lo AS foo;
WITH table_b AS (
    SELECT
        id,
        name
    FROM
        table_x
    WHERE
        id > 100
) UPDATE weather
SET
    temp_lo = temp_lo + 1
RETURNING *;
