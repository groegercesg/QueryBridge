SELECT 
    ( COUNT ( o_custkey ) + AVG ( o_totalprice ) ) / ( SUM(o_orderkey) + MIN(o_shippriority) ) * 25 as fun_aggregate,
    ( MAX ( o_custkey ) * MIN ( o_totalprice ) ) * ( MAX(o_orderkey) - AVG(o_shippriority) ) - -5 as massive_query 
FROM 
    orders;