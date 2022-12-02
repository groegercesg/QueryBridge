EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) 
SELECT
    s_suppkey
FROM
    supplier
WHERE
    s_comment LIKE '_r%';