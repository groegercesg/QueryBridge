SELECT
  t24.c_name,
  t24.o_custkey,
  t24.o_orderkey,
  t24.o_orderdate,
  t24.o_totalprice,
  sum(t24.l_quantity)
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          t22.c_name,
          t22.o_custkey,
          t22.o_orderkey,
          t22.o_orderdate,
          t22.o_totalprice
        FROM
          (
            SELECT
              *
            FROM
              (
                SELECT
                  t20.c_custkey,
                  t20.c_name
                FROM
                  (
                    SELECT
                      *
                    FROM
                      customer t3
                  ) t20
              ) t20
              inner JOIN (
                SELECT
                  *
                FROM
                  (
                    SELECT
                      *
                    FROM
                      orders t6
                  ) t6
                  inner JOIN (
                    SELECT
                      t18.l_orderkey
                    FROM
                      (
                        SELECT
                          t17.l_orderkey
                        FROM
                          (
                            SELECT
                              t16.l_orderkey,
                              sum(t16.l_quantity) as sum_quantity
                            FROM
                              (
                                SELECT
                                  *
                                FROM
                                  lineitem t7
                              ) t16
                            GROUP BY
                              t16.l_orderkey
                            HAVING
                              sum(t16.l_quantity) > 300
                          ) t17
                      ) t18
                    GROUP BY
                      t18.l_orderkey
                  ) t18 ON t6.o_orderkey = t18.l_orderkey
              ) t19 ON t20.c_custkey = t19.o_custkey
          ) t22
      ) t22
      inner JOIN (
        SELECT
          *
        FROM
          lineitem t7
      ) t7 ON t22.o_orderkey = t7.l_orderkey
  ) t24 GROUP BY t24.c_name,
  t24.o_custkey,
  t24.o_orderkey,
  t24.o_orderdate,
  t24.o_totalprice;