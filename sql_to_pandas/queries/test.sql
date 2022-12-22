SELECT
    (100.00 * SUM(
        CASE WHEN l_extendedprice <= 2500
            THEN l_extendedprice * (1 - l_discount)
        ELSE
            0
        END
    ) / SUM(l_extendedprice * (1 - l_discount))) AS promo_revenue
FROM
    lineitem;