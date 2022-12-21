SELECT s_suppkey,
CASE
    WHEN s_nationkey > 0
        AND s_nationkey <= 50 THEN 'Young Nation'
    WHEN s_nationkey > 50
        AND s_nationkey <= 120 THEN 'Medium Nation'
    WHEN s_nationkey> 120 THEN 'Old Nation'
END as case_column, s_nationkey
FROM supplier
ORDER BY s_suppkey;