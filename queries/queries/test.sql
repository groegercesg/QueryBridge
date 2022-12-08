select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) * avg(l_discount * 0.5) - 72 as sum_qty,
    sum(l_extendedprice) / ((count(l_quantity) - min(l_discount) - min(l_tax)) * 0.5) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
from
    lineitem
group by
    l_returnflag,
    l_linestatus;