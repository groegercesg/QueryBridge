SELECT
    s_suppkey
FROM
    supplier
WHERE
    s_comment LIKE '%Customer%Complaints%';