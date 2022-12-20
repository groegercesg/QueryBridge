

"""
Examples from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-case/

Simple CASE
SELECT s_suppkey,
       CASE WHEN s_nationkey=17 THEN 'BOTTLE'
            WHEN s_nationkey=5 THEN 'BAG'
            ELSE 'NEITHER'
       END as item
    FROM supplier;
    
More tricky CASE
SELECT s_suppkey,
       CASE WHEN s_nationkey=17 THEN 'BOTTLE'
            WHEN s_nationkey=5 THEN 'BAG'
            WHEN s_nationkey=3 THEN 'CASE'
            WHEN s_nationkey <> 123 THEN 'MAGIC'
            WHEN s_nationkey > 0 THEN 'BIGO'
       END as item, s_nationkey
    FROM supplier;

CASE with any conditions
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

CASE with OR conditions
SELECT s_suppkey,
       CASE
           WHEN s_nationkey > 0
                AND s_nationkey < 20 THEN 'Young Nation'
           WHEN s_nationkey > 20
                OR s_nationkey = 20 THEN 'Medium and Above Nation'
       END as case_column, s_nationkey
FROM supplier
ORDER BY s_suppkey;

CASE nested inside an (non-group) Aggregation
SELECT
	SUM (CASE
               WHEN s_acctbal < 1000 THEN 1
	       ELSE 0
	      END
	) AS "Risky",
	SUM (
		CASE
		WHEN s_acctbal >= 1000 and s_acctbal < 5000 THEN 1
		ELSE 0
		END
	) AS "Normal",
	SUM (
		CASE
		WHEN s_acctbal >= 5000 THEN 1
		ELSE 0
		END
	) AS "Bloated"
FROM
	supplier;

Simple CASE expression
SELECT p_partkey,
       p_name,
       CASE p_container
           WHEN 'JUMBO PKG' THEN 'Jumbo Package'
           WHEN 'SM PKG' THEN 'Small Package'
           ELSE 'Other'
       END as container_annotation
FROM part
ORDER BY p_partkey;

CASE with nested LIKE
SELECT p_partkey,
       p_name,
       CASE 
           WHEN p_container LIKE '%PKG%' THEN 'Package'
           WHEN p_container LIKE '%CASE%' THEN 'Case'
           WHEN p_container LIKE '%BAG%' THEN 'Bag'
           WHEN p_container LIKE '%DRUM%' THEN 'Drum'
           WHEN p_container LIKE '%%BOX%' THEN 'Box'
           WHEN p_container LIKE '%JAR%' THEN 'Jar'
           WHEN p_container LIKE '%PACK%' THEN 'Pack'
           ELSE 'Other'
       END as container_format, p_container
FROM part
ORDER BY p_partkey;

CASE with nested LIKE, inside an GROUP
SELECT
    container_format,
    count(*) as container_count
FROM (
        SELECT p_partkey,
            p_name,
            CASE 
                WHEN p_container LIKE '%PKG%' THEN 'Package'
                WHEN p_container LIKE '%CASE%' THEN 'Case'
                WHEN p_container LIKE '%BAG%' THEN 'Bag'
                WHEN p_container LIKE '%DRUM%' THEN 'Drum'
                WHEN p_container LIKE '%%BOX%' THEN 'Box'
                WHEN p_container LIKE '%JAR%' THEN 'Jar'
                WHEN p_container LIKE '%PACK%' THEN 'Pack'
                WHEN p_container LIKE '%CAN%' THEN 'Can'
                ELSE 'Other'
            END as container_format, p_container
        FROM part
        ORDER BY p_partkey
    ) as container_agg
GROUP BY 
    container_format;
    
CASE with nested LIKE, inside a filter
SELECT
    p_partkey,p_name,container_format, p_container
FROM (
        SELECT p_partkey,
            p_name,
            CASE 
                WHEN p_container LIKE '%PKG%' THEN 'Package'
                WHEN p_container LIKE '%CASE%' THEN 'Case'
                WHEN p_container LIKE '%BAG%' THEN 'Bag'
                WHEN p_container LIKE '%DRUM%' THEN 'Drum'
                WHEN p_container LIKE '%%BOX%' THEN 'Box'
                WHEN p_container LIKE '%JAR%' THEN 'Jar'
                WHEN p_container LIKE '%PACK%' THEN 'Pack'
                WHEN p_container LIKE '%CAN%' THEN 'Can'
                ELSE 'Other'
            END as container_format, p_container
        FROM part
        ORDER BY p_partkey
    ) as container_agg
WHERE container_format = 'Other';

CASE with nested expression, we should use the expression tree for this one!
SELECT
    (100.00 * SUM(
        CASE WHEN l_extendedprice <= 2500' 
            THEN l_extendedprice * (1 - l_discount) 
        ELSE 
            0 
        END
    ) / SUM(l_extendedprice * (1 - l_discount))) AS promo_revenue
FROM
    lineitem;

"""