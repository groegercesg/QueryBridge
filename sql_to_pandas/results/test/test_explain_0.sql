EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) 
select count( distinct ( o_custkey ) ) from orders;