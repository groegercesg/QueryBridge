WITH "_u_0" AS (
  SELECT
    MIN(partsupp.ps_supplycost) AS "_col_0",
    partsupp.ps_partkey AS _u_1
  FROM partsupp
  JOIN region
    ON region.r_name = 'EUROPE'
  JOIN nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN supplier
    ON supplier.s_nationkey = nation.n_nationkey
    AND supplier.s_suppkey = partsupp.ps_suppkey
  GROUP BY
    partsupp.ps_partkey
)
SELECT
  supplier.s_acctbal AS s_acctbal,
  supplier.s_name AS s_name,
  nation.n_name AS n_name,
  part.p_partkey AS p_partkey,
  part.p_mfgr AS p_mfgr,
  supplier.s_address AS s_address,
  supplier.s_phone AS s_phone,
  supplier.s_comment AS s_comment
FROM part
LEFT JOIN "_u_0" AS "_u_0"
  ON part.p_partkey = "_u_0"."_u_1"
JOIN region
  ON region.r_name = 'EUROPE'
JOIN nation
  ON nation.n_regionkey = region.r_regionkey
JOIN partsupp
  ON part.p_partkey = partsupp.ps_partkey
JOIN supplier
  ON supplier.s_nationkey = nation.n_nationkey
  AND supplier.s_suppkey = partsupp.ps_suppkey
WHERE
  part.p_size = 15
  AND part.p_type LIKE '%BRASS'
  AND partsupp.ps_supplycost = "_u_0"."_col_0"
  AND NOT "_u_0"."_u_1" IS NULL
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 100;