SELECT
  t13.o_orderpriority,
  count(t13.o_orderdate)
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          *
        FROM
          (
            SELECT
              *
            FROM
              orders t6
            ORDER BY
              t6.o_orderkey
          ) t11
        WHERE
          t11.o_orderdate >= '1993-07-01'
          and t11.o_orderdate < '1993-10-01'
        ORDER BY
          t11.o_orderkey
      ) t11
      inner JOIN (
        SELECT
          t10.l_orderkey
        FROM
          (
            SELECT
              t9.l_orderkey
            FROM
              (
                SELECT
                  *
                FROM
                  (
                    SELECT
                      *
                    FROM
                      lineitem t7
                    ORDER BY
                      t7.l_orderkey
                  ) t8
                WHERE
                  t8.l_commitdate < t8.l_receiptdate
                ORDER BY
                  t8.l_orderkey
              ) t9
            ORDER BY
              t9.l_orderkey
          ) t10
        GROUP BY
          t10.l_orderkey
        ORDER BY
          t10.l_orderkey
      ) t10 ON t11.o_orderkey = t10.l_orderkey
    ORDER BY
      t11.o_orderkey
  ) t13 GROUP BY t13.o_orderpriority ORDER BY t13.o_orderpriority;