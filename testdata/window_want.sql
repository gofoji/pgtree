SELECT
    depname,
    empno,
    salary,
    rank() OVER (PARTITION BY depname ORDER BY salary DESC)
FROM
    empsalary;
SELECT
    salary,
    sum(salary) OVER ()
FROM
    empsalary;
SELECT
    sum(salary) OVER w,
    avg(salary) OVER w
FROM
    empsalary
WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
