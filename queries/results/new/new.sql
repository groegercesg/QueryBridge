SELECT
    s_suppkey
FROM
    supplier
WHERE
    s_address NOT IN ('Germany', 'France', 'UK');