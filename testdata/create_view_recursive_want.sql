CREATE VIEW reporting_line(employee_id, subordinates) AS
WITH RECURSIVE reporting_line(employee_id, subordinates) AS (
    SELECT
        employee_id,
        full_name AS subordinates
    FROM
        employees
    WHERE
        manager_id IS NULL
    UNION ALL
    SELECT
        e.employee_id,
        rl.subordinates || ' > ' || e.full_name AS subordinates
    FROM
        employees e
        JOIN reporting_line rl ON e.manager_id = rl.employee_id
    UNION
    SELECT e, y FROM zz
    INTERSECT ALL
    SELECT * FROM cow
    EXCEPT
    SELECT a, b FROM c
)
SELECT
    employee_id,
    subordinates
FROM
    reporting_line;
