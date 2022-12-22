import pytest
import os
import inspect

"""
Examples from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-case/
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

def test_case_simple():
    # Simple CASE
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        supplier['item'] = np.select([supplier["s_nationkey"] == 17, supplier["s_nationkey"] == 5], ['BOTTLE', 'BAG'], 'NEITHER')
        df_filter_1 = supplier[['s_suppkey', 'item']]
        return df_filter_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT s_suppkey,
        CASE WHEN s_nationkey=17 THEN 'BOTTLE'
            WHEN s_nationkey=5 THEN 'BAG'
            ELSE 'NEITHER'
        END as item
        FROM supplier;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_intermediate():
    # More tricky CASE
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        supplier['item'] = np.select([supplier["s_nationkey"] == 17, supplier["s_nationkey"] == 5, supplier["s_nationkey"] == 3, supplier["s_nationkey"] != 123, supplier["s_nationkey"] > 0], ['BOTTLE', 'BAG', 'CASE', 'MAGIC', 'BIGO'], 'NULL')
        df_filter_1 = supplier[['s_suppkey', 'item', 's_nationkey']]
        return df_filter_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT s_suppkey,
        CASE WHEN s_nationkey=17 THEN 'BOTTLE'
            WHEN s_nationkey=5 THEN 'BAG'
            WHEN s_nationkey=3 THEN 'CASE'
            WHEN s_nationkey <> 123 THEN 'MAGIC'
            WHEN s_nationkey > 0 THEN 'BIGO'
        END as item, s_nationkey
        FROM supplier;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_conditions():
    # CASE with any conditions
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        supplier['case_column'] = np.select([(supplier['s_nationkey'] > 0) & (supplier['s_nationkey'] <= 50), (supplier['s_nationkey'] > 50) & (supplier['s_nationkey'] <= 120), supplier["s_nationkey"] > 120], ['Young Nation', 'Medium Nation', 'Old Nation'], 'NULL')
        df_filter_1 = supplier[['s_suppkey', 'case_column', 's_nationkey']]
        return df_filter_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT s_suppkey,
        CASE
            WHEN s_nationkey > 0
                AND s_nationkey <= 50 THEN 'Young Nation'
            WHEN s_nationkey > 50
                AND s_nationkey <= 120 THEN 'Medium Nation'
            WHEN s_nationkey> 120 THEN 'Old Nation'
        END as case_column, s_nationkey
        FROM supplier
        ORDER BY s_suppkey;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_or_conditions():
    # CASE with OR conditions
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        supplier['case_column'] = np.select([(supplier['s_nationkey'] > 0) & (supplier['s_nationkey'] < 20), (supplier['s_nationkey'] > 20) | (supplier['s_nationkey'] == 20)], ['Young Nation', 'Medium and Above Nation'], 'NULL')
        df_filter_1 = supplier[['s_suppkey', 'case_column', 's_nationkey']]
        return df_filter_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT s_suppkey,
        CASE
            WHEN s_nationkey > 0
                AND s_nationkey < 20 THEN 'Young Nation'
            WHEN s_nationkey > 20
                OR s_nationkey = 20 THEN 'Medium and Above Nation'
        END as case_column, s_nationkey
        FROM supplier
        ORDER BY s_suppkey;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_aggr_nested():
    # CASE nested inside an (non-group) Aggregation
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_filter_1['case_a'] = np.where(df_filter_1["s_acctbal"] < 1000, 1, 0)
        df_filter_1['case_b'] = np.where((df_filter_1['s_acctbal'] >= 1000) & (df_filter_1['s_acctbal'] < 5000), 1, 0)
        df_filter_1['case_c'] = np.where(df_filter_1["s_acctbal"] >= 5000, 1, 0)
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['Risky'] = [(df_filter_1.case_a).sum()]
        df_aggr_1['Normal'] = [(df_filter_1.case_b).sum()]
        df_aggr_1['Bloated'] = [(df_filter_1.case_c).sum()]
        df_aggr_1 = df_aggr_1[['Risky', 'Normal', 'Bloated']]
        return df_aggr_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT
            SUM (
                CASE
                WHEN s_acctbal < 1000 THEN 1
                ELSE 0
                END
            ) AS "Risky",
            SUM (
                CASE
                WHEN s_acctbal >= 1000 and s_acctbal < 5000 THEN 1
                ELSE 0
                END
            ) AS "Normal",
            SUM (
                CASE
                WHEN s_acctbal >= 5000 THEN 1
                ELSE 0
                END
            ) AS "Bloated"
        FROM
            supplier;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_expression():
    # Simple CASE expression
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        part['container_annotation'] = np.select([part['p_container'] == 'JUMBO PKG', part['p_container'] == 'SM PKG'], ['Jumbo Package', 'Small Package'], 'Other')
        df_filter_1 = part[['p_partkey', 'p_name', 'container_annotation']]
        return df_filter_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT 
            p_partkey,
            p_name,
            CASE p_container
                WHEN 'JUMBO PKG' THEN 'Jumbo Package'
                WHEN 'SM PKG' THEN 'Small Package'
                ELSE 'Other'
            END as container_annotation
        FROM part
        ORDER BY p_partkey;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_expression_nested_like():
    # CASE with nested LIKE
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        part['container_format'] = np.select([part.p_container.str.contains("^.*?PKG.*?$",regex=True), part.p_container.str.contains("^.*?CASE.*?$",regex=True), part.p_container.str.contains("^.*?BAG.*?$",regex=True), part.p_container.str.contains("^.*?DRUM.*?$",regex=True), part.p_container.str.contains("^.*?.*?BOX.*?$",regex=True), part.p_container.str.contains("^.*?JAR.*?$",regex=True), part.p_container.str.contains("^.*?PACK.*?$",regex=True)], ['Package', 'Case', 'Bag', 'Drum', 'Box', 'Jar', 'Pack'], 'Other')
        df_filter_1 = part[['p_partkey', 'p_name', 'container_format', 'p_container']]
        return df_filter_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT 
            p_partkey,
            p_name,
            CASE 
                WHEN p_container LIKE '%PKG%' THEN 'Package'
                WHEN p_container LIKE '%CASE%' THEN 'Case'
                WHEN p_container LIKE '%BAG%' THEN 'Bag'
                WHEN p_container LIKE '%DRUM%' THEN 'Drum'
                WHEN p_container LIKE '%%BOX%' THEN 'Box'
                WHEN p_container LIKE '%JAR%' THEN 'Jar'
                WHEN p_container LIKE '%PACK%' THEN 'Pack'
                ELSE 'Other'
            END as container_format, 
            p_container
        FROM part
        ORDER BY p_partkey;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected

def test_case_expression_nested_like_group():
    # CASE with nested LIKE, inside an GROUP
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        part['container_format'] = np.select([part.p_container.str.contains("^.*?PKG.*?$",regex=True), part.p_container.str.contains("^.*?CASE.*?$",regex=True), part.p_container.str.contains("^.*?BAG.*?$",regex=True), part.p_container.str.contains("^.*?DRUM.*?$",regex=True), part.p_container.str.contains("^.*?.*?BOX.*?$",regex=True), part.p_container.str.contains("^.*?JAR.*?$",regex=True), part.p_container.str.contains("^.*?PACK.*?$",regex=True), part.p_container.str.contains("^.*?CAN.*?$",regex=True)], ['Package', 'Case', 'Bag', 'Drum', 'Box', 'Jar', 'Pack', 'Can'], 'Other')
        df_filter_1 = part[['p_partkey', 'container_format']]
        df_group_1 = df_filter_1 \\
            .groupby(['container_format'], sort=False) \\
            .agg(
                container_count=("container_format", "count"),
            )
        df_group_1 = df_group_1[['container_count']]
        return df_group_1""").strip()
    
    sql_query = inspect.cleandoc("""
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
            container_format;""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_expression_nested_like_filter():
    # CASE with nested LIKE, inside a filter
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        part['container_format'] = np.select([part.p_container.str.contains("^.*?PKG.*?$",regex=True), part.p_container.str.contains("^.*?CASE.*?$",regex=True), part.p_container.str.contains("^.*?BAG.*?$",regex=True), part.p_container.str.contains("^.*?DRUM.*?$",regex=True), part.p_container.str.contains("^.*?.*?BOX.*?$",regex=True), part.p_container.str.contains("^.*?JAR.*?$",regex=True), part.p_container.str.contains("^.*?PACK.*?$",regex=True), part.p_container.str.contains("^.*?CAN.*?$",regex=True)], ['Package', 'Case', 'Bag', 'Drum', 'Box', 'Jar', 'Pack', 'Can'], 'Other')
        df_filter_1 = part[part.container_format == 'Other']
        df_filter_1 = df_filter_1[['p_partkey', 'p_name', 'container_format', 'p_container']]
        df_sort_1 = df_filter_1.sort_values(by=['p_partkey'], ascending=[True])
        df_sort_1 = df_sort_1[['p_partkey', 'p_name', 'container_format', 'p_container']]
        return df_sort_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT
            p_partkey, p_name, container_format, p_container
        FROM (
            SELECT p_partkey,
                p_name,
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
                END as container_format, p_container
            FROM part
            ORDER BY p_partkey
            ) as container_agg
        WHERE 
            container_format = 'Other';""").strip()
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_case_maths_expressions():
    # CASE with nested expression, we should use the expression tree for this one!
    
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = lineitem[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
        df_filter_1['case_a'] = np.where(df_filter_1["l_extendedprice"] <= 2500, ( df_filter_1["l_extendedprice"] * ( 1 - df_filter_1["l_discount"] )), 0)
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['promo_revenue'] = [((100.00 * (df_filter_1.case_a).sum()) / ((df_filter_1.l_extendedprice) * (1 - (df_filter_1.l_discount))).sum())]
        df_aggr_1 = df_aggr_1[['promo_revenue']]
        return df_aggr_1""").strip()
    
    sql_query = inspect.cleandoc("""
        SELECT
            (100.00 * SUM(
                CASE WHEN l_extendedprice <= 2500
                    THEN l_extendedprice * (1 - l_discount) 
                ELSE 
                    0 
                END
            ) / SUM(l_extendedprice * (1 - l_discount))) AS promo_revenue
        FROM
            lineitem;""").strip()
    
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