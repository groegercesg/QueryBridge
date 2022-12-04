EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) 
select distinct ( o_custkey ) from orders limit 10;