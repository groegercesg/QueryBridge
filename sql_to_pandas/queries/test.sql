SELECT
    container_format,
    count(*) as container_count
FROM (
    SELECT p_partkey,
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
        END as container_format
    FROM part
    ORDER BY p_partkey
    ) as container_agg
GROUP BY
    container_format;