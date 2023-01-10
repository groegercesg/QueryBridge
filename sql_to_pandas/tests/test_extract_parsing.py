import pytest
import os
import inspect

"""
Examples from: W3_Schools, https://www.postgresqltutorial.com/postgresql-date-functions/postgresql-extract/
"""

# Testing methods
from general_test_functions import *

# Variable Names
constants = Constants_class("testing_inputs/", "testing_outputs/", "query.sql", "../sql_to_pandas.py", "query.py")

# Before all
@pytest.fixture(scope='module', autouse=True)
def setup():
    print("Run Setup")
    # Create directory for tests if doesn't already exist
    for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
        os.makedirs(dir, exist_ok=True)  # succeeds even if directory exists.
    
#  Before and after each
@pytest.fixture(autouse=True)
def run_around_tests():
    # Delete in Dir
    for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
        delete_files_in_dir(dir)
    # Code that will run before your test, for example:
    files_before_input = count_files_in_directory(constants.INPUTS_DIR)
    files_before_output = count_files_in_directory(constants.OUTPUTS_DIR)
    
    yield
    
    # Delete in Dir
    for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
        delete_files_in_dir(dir)
    # Code that will run after your test, for example:
    files_after_input = count_files_in_directory(constants.INPUTS_DIR)
    files_after_output = count_files_in_directory(constants.OUTPUTS_DIR)
    
    assert files_before_input == files_after_input
    assert files_before_output == files_after_output

def test_extract_simple():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        select
            extract(year from o_orderdate) as o_year
        from
            orders;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_hour_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        select extract(hour from timestamp '2002-09-17 19:27:45') as hour_part;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_month_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        select extract(month from interval '3 years 7 months') as month_part;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_year_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_quarter_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(QUARTER FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_month_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_day_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(DAY FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_century_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(CENTURY FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_decade_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(DECADE FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_day_of_week_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(DOW FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_day_of_year_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(DOY FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_epoch_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(EPOCH FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_hour_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_minute_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MINUTE FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_second_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(SECOND FROM TIMESTAMP '2016-12-31 13:30:15.45');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_weekday_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(ISODOW FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_millisecond_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MILLISECONDS FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_microsecond_from_timestamp():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MICROSECONDS FROM TIMESTAMP '2016-12-31 13:30:15');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_year_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(YEAR FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_quarter_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(QUARTER FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_month_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MONTH FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_day_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(DAY FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_hour_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(HOUR FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_minute_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MINUTE FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_second_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(SECOND FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_milliseconds_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MILLISECONDS FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_microseconds_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MICROSECONDS FROM INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_decade_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(DECADE FROM INTERVAL '60 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_millennium_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(MILLENNIUM FROM INTERVAL '1999 years 5 months 4 days 3 hours 2 minutes 1 second');""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_century_from_interval():    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        """).strip()
    
    sql_query = inspect.cleandoc("""
        SELECT EXTRACT(CENTURY FROM INTERVAL '1999 years 5 months 4 days 3 hours 2 minutes 1 second' ); """).strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_extract_query_7():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = customer[['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']]
        df_filter_2 = nation[(nation.n_name == 'GERMANY') | (nation.n_name == 'FRANCE')]
        df_filter_2 = df_filter_2[['n_name', 'n_nationkey']]
        df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['c_nationkey'], right_on=['n_nationkey'])
        df_merge_1 = df_merge_1[['c_custkey', 'n_name']]
        df_filter_3 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
        df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['c_custkey'], right_on=['o_custkey'])
        df_merge_2 = df_merge_2[['o_orderkey', 'n_name']]
        df_filter_4 = lineitem[(lineitem.l_shipdate >= pd.Timestamp('1995-01-01 00:00:00')) & (lineitem.l_shipdate <= pd.Timestamp('1996-12-31 00:00:00'))]
        df_filter_4 = df_filter_4[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
        df_merge_3 = df_merge_2.merge(df_filter_4, left_on=['o_orderkey'], right_on=['l_orderkey'])
        df_merge_3 = df_merge_3[['l_shipdate', 'l_extendedprice', 'l_discount', 'l_suppkey', 'n_name']]
        df_sort_1 = df_merge_3.sort_values(by=['l_suppkey'], ascending=[True])
        df_sort_1 = df_sort_1[['l_shipdate', 'l_extendedprice', 'l_discount', 'l_suppkey', 'n_name']]
        df_filter_5 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_filter_6 = nation[(nation.n_name == 'FRANCE') | (nation.n_name == 'GERMANY')]
        df_filter_6 = df_filter_6[['n_name', 'n_nationkey']]
        df_merge_4 = df_filter_5.merge(df_filter_6, left_on=['s_nationkey'], right_on=['n_nationkey'])
        df_merge_4 = df_merge_4[['s_suppkey', 'n_name']]
        df_sort_2 = df_merge_4.sort_values(by=['s_suppkey'], ascending=[True])
        df_sort_2 = df_sort_2[['s_suppkey', 'n_name']]
        df_merge_5 = df_sort_1.merge(df_sort_2, left_on=['l_suppkey'], right_on=['s_suppkey'])
        df_merge_5['l_year'] = df_merge_5.l_shipdate.dt.year
        df_merge_5 = df_merge_5[((df_merge_5.n_name_y == 'FRANCE') & (df_merge_5.n_name_x == 'GERMANY')) | ((df_merge_5.n_name_y == 'GERMANY') & (df_merge_5.n_name_x == 'FRANCE'))]
        df_merge_5 = df_merge_5[['n_name_y', 'n_name_x', 'l_year', 'l_extendedprice', 'l_discount']]
        df_merge_5['supp_nation'] = df_merge_5.n_name_y
        df_merge_5['cust_nation'] = df_merge_5.n_name_x
        df_sort_3 = df_merge_5.sort_values(by=['supp_nation', 'cust_nation', 'l_year'], ascending=[True, True, True])
        df_sort_3 = df_sort_3[['supp_nation', 'cust_nation', 'l_year', 'l_extendedprice', 'l_discount']]
        df_sort_3['volume'] = ((df_sort_3.l_extendedprice) * (1 - (df_sort_3.l_discount)))
        df_group_1 = df_sort_3 \\
            .groupby(['supp_nation', 'cust_nation', 'l_year'], sort=False) \\
            .agg(
                revenue=("volume", "sum"),
            )
        df_group_1 = df_group_1[['revenue']]
        df_limit_1 = df_group_1[['revenue']]
        df_limit_1 = df_limit_1.head(1)
        return df_limit_1""").strip()
    
    sql_query = inspect.cleandoc("""
        select
                supp_nation,
                cust_nation,
                l_year,
                sum(volume) as revenue
        from
                (
                        select
                                n1.n_name as supp_nation,
                                n2.n_name as cust_nation,
                                extract(year from l_shipdate) as l_year,
                                l_extendedprice * (1 - l_discount) as volume
                        from
                                supplier,
                                lineitem,
                                orders,
                                customer,
                                nation n1,
                                nation n2
                        where
                                s_suppkey = l_suppkey
                                and o_orderkey = l_orderkey
                                and c_custkey = o_custkey
                                and s_nationkey = n1.n_nationkey
                                and c_nationkey = n2.n_nationkey
                                and (
                                        (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                                        or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                                )
                                and l_shipdate between date '1995-01-01' and date '1996-12-31'
                ) as shipping
        group by
                supp_nation,
                cust_nation,
                l_year
        order by
                supp_nation,
                cust_nation,
                l_year
        LIMIT 1;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected

    
# After all
@pytest.fixture(scope="module", autouse=True)
def cleanup(request):
    """Cleanup a testing directory once we are finished."""
    def remove_cleanup():
        for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
            remove_dir(dir)
    request.addfinalizer(remove_cleanup)

