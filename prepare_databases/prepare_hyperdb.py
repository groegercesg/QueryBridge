from prepare_databases.prepare_database import PrepareDatabase
from tableauhyperapi import HyperProcess, Telemetry, CreateMode, Connection, TableDefinition, SqlType, NOT_NULLABLE
from enum import Enum
from collections import defaultdict
from difflib import SequenceMatcher
import json

class DatabaseReference():        
    def process_keys(self, keys):
        assert isinstance(keys, list), f"Unexpected primary_keys list format. It was of type: { type(keys) }"
        formatted_keys = [str(e) for e in keys]
        return "(" + ', '.join(formatted_keys) + ")"
        
class PrimaryKey(DatabaseReference):
    def __init__(self, table_name, primary_keys):
        self.table_name = table_name
        self.primary_keys = self.process_keys(primary_keys)
    
class ForeignKey(DatabaseReference):
    def __init__(self, table, keys, ref_table, ref_keys):
        assert len(keys) == len(ref_keys), "Foreign key references must have the same amount of keys."
        self.table_name = table
        self.table_keys = self.process_keys(keys)
        self.ref_table_name = ref_table
        self.ref_table_keys = self.process_keys(ref_keys)

class PrepareHyperDB(PrepareDatabase):
    def __init__(self, connection_details):
        super().__init__(connection_details, "Hyper DB")
        self.explain_options = "EXPLAIN (VERBOSE)"
        self.hyper_parameters = {
            #"log_config": "",
            "max_query_size": "10000000000",
            "hard_concurrent_query_thread_limit": "1" ## Change me back!
        }

    def is_database_empty(self):
        tables = []
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=self.hyper_parameters) as hyper:
            with Connection(hyper.endpoint, self.connection_details, CreateMode.NONE) as connection:
                catalog = connection.catalog
                schemas = catalog.get_schema_names()
                for schema_name in schemas:
                    tables.extend(catalog.get_table_names(schema_name))
        if len(tables) > 0:
            return False
        else:
            print(tables)
            return True
        
    def get_table_keys(self):
        # Return a structure that looks like
        # Map['table_name', tuple]
        #   tuple(set(primary_keys), set(foreign_keys), set(other_columns))
        
        table_keys_dict = {}
        
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=self.hyper_parameters) as hyper:
            with Connection(hyper.endpoint, self.connection_details, CreateMode.NONE) as connection:
                schema_name = connection.catalog.get_schema_names()[0]
                table_names = connection.catalog.get_table_names(schema_name)
                for table_name in table_names:
                    key_content = list()
                    
                    # Get sets for primary and foreign keys
                    # Do Primary Keys
                    primary_keys = connection.execute_list_query(f"""
                            SELECT a.attname
                            FROM
                                pg_catalog.pg_constraint AS c
                                CROSS JOIN LATERAL UNNEST(c.conkey) AS cols(colnum) -- conkey is a list of the columns of the constraint; so we split it into rows so that we can join all column numbers onto their names in pg_attribute
                                INNER JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.conrelid AND cols.colnum = a.attnum
                            WHERE
                                c.contype = 'p' -- p = primary key constraint, f = foreign key constraint
                                AND c.conrelid = '{schema_name.name.unescaped}.{table_name.name.unescaped}'::regclass
                        """)
                    
                    # Flatten nested list
                    primary_keys = [item for sublist in primary_keys for item in sublist]
                    
                    primary_key_set = set()
                    primary_key_set.add(tuple(primary_keys))
                    
                    key_content.append(
                        primary_key_set
                    )
                    
                    # Do Foreign Keys
                    foreign_keys = connection.execute_list_query(f"""
                            SELECT conname AS constraint_name, pg_m.relname AS table_name, ta.attname AS column_name,
        pg_a.relname AS foreign_table_name, fa.attname AS foreign_column_name
  FROM (
   SELECT conname, conrelid, confrelid,
          unnest(conkey) AS conkey, unnest(confkey) AS confkey
     FROM pg_catalog.pg_constraint
    WHERE conrelid = '{schema_name.name.unescaped}.{table_name.name.unescaped}'::regclass
      --and contype = 'f'
  ) sub
  JOIN pg_catalog.pg_attribute AS ta ON ta.attrelid = conrelid AND ta.attnum = conkey
  JOIN pg_catalog.pg_attribute AS fa ON fa.attrelid = confrelid AND fa.attnum = confkey  
  JOIN pg_catalog.pg_class AS pg_m ON pg_m.oid = conrelid   
  JOIN pg_catalog.pg_class AS pg_a ON pg_a.oid = confrelid                                 
                        """)
                    
                    if len(foreign_keys) == 0:
                        key_content.append(
                            dict()
                        )
                    else:
                        # Look at unique key contraints
                        uniqueFirstColumn = set([row[0] for row in foreign_keys])
                        mapDict = dict()
                        if len(uniqueFirstColumn) == len(foreign_keys):
                            for fkey in foreign_keys:
                                mapDict[fkey[2]] = (fkey[4], fkey[3])
                        else:
                            # We have to do some grouping here!
                            fromKeys = defaultdict(set)
                            toKeys = defaultdict(set)
                            toTables = defaultdict(set)
                            for fkey in foreign_keys:
                                fromKeys[fkey[0]].add(fkey[2])
                                toKeys[fkey[0]].add(fkey[4])
                                toTables[fkey[0]].add(fkey[3])
                                
                            # A Compound FKey should only map to one table
                            assert all(len(x) == 1 for x in toTables.values())
                            
                            # Add to mapDict
                            for key in fromKeys.keys():
                                # Sort alphabetically
                                newFromKey = tuple(sorted(list(fromKeys[key])))
                                if len(newFromKey) == 1:
                                    newFromKey = newFromKey[0]
                                newToKeys = tuple(sorted(list(toKeys[key])))
                                mapDict[newFromKey] = (list(toTables[key])[0], newToKeys)
                            
                            # Special hack for TPC-H: 
                            for _, _, fromKey, toTable, toKey in foreign_keys:
                                def similar(a, b):
                                    return SequenceMatcher(None, a, b).ratio()
                                if similar(fromKey, toKey) > 0.8:
                                    mapDict[fromKey] = (toTable, toKey)
                            
                        key_content.append(
                            mapDict
                        )
                    
                    
                    assert len(key_content) == 2
                    
                    # Get the other columns for table
                    all_columns = [item for sublist in connection.execute_list_query(f"""
                        SELECT a.attname
                        FROM
                            pg_catalog.pg_attribute AS a
                        WHERE
                            a.attrelid = '{schema_name.name.unescaped}.{table_name.name.unescaped}'::regclass
                        """)
                    for item in sublist]
                    key_content.append(all_columns)
                    
                    assert len(key_content) == 3
                    # Add to dictionary
                    table_keys_dict[table_name.name.unescaped] = tuple(key_content)
                    
        return table_keys_dict
    
    def execute_query(self, query_text):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=self.hyper_parameters) as hyper:
            with Connection(hyper.endpoint, self.connection_details, CreateMode.NONE) as connection:
                return connection.execute_list_query(query_text)

    def get_explain(self, query_text, query_name=None):
        query_list = []
        if isinstance(query_text, list):
            query_list = [self.create_explain(query_text)]
        elif isinstance(query_text, str) and self.explain_options not in query_text:
            # Handle multi-part queries
            if query_text.count(";") in [0, 1]:
                query_list = [f"{self.explain_options}\n{query_text}"]
            else:
                split_query = query_text.replace("\n", " ").replace('\t', " ").split(";")
                for idx, query in enumerate(split_query):
                    query = query.strip()
                    if query[:6] == "select":
                        split_query[idx] = f"{self.explain_options}\n{query}"
                    else:
                        split_query[idx] = query
                query_list = list(filter(None, split_query))
        else:
            raise Exception(f"Unknown format for query_text, it was of type: {type(query_text)}")
        
        query_pre_json = ""
        for query in query_list:
            if query.split(" ")[0].lower() == "explain":
                # Run the execution, and process into a JSON
                for query_element in self.execute_query(query):
                    if isinstance(query_element, list) and len(query_element) == 1:
                        query_pre_json += query_element[0]
                    else:
                        raise Exception(f"The query element was an unknown type ({type(query_element)}) or an unexpected length ({len(query_element)}).")
            else:
                self.execute_query(query)
        
        query_json = json.loads(query_pre_json)
        return query_json, query_text
    
    def __create_tpch_table_definitions(self):
        # Keys that we need to reference in many places
        region_r_regionkey = TableDefinition.Column('r_regionkey', SqlType.int(), nullability=NOT_NULLABLE)
        nation_n_nationkey = TableDefinition.Column('n_nationkey', SqlType.int(), nullability=NOT_NULLABLE)
        nation_n_regionkey = TableDefinition.Column('n_regionkey', SqlType.int(), nullability=NOT_NULLABLE)
        supplier_s_suppkey = TableDefinition.Column('s_suppkey', SqlType.int(), nullability=NOT_NULLABLE)
        supplier_s_nationkey = TableDefinition.Column('s_nationkey', SqlType.int(), nullability=NOT_NULLABLE)
        customer_c_custkey = TableDefinition.Column('c_custkey', SqlType.int(), nullability=NOT_NULLABLE)
        customer_c_nationkey = TableDefinition.Column('c_nationkey', SqlType.int(), nullability=NOT_NULLABLE)
        part_p_partkey = TableDefinition.Column('p_partkey', SqlType.int(), nullability=NOT_NULLABLE)
        partsupp_ps_partkey = TableDefinition.Column('ps_partkey', SqlType.int(), nullability=NOT_NULLABLE)
        partsupp_ps_suppkey = TableDefinition.Column('ps_suppkey', SqlType.int(), nullability=NOT_NULLABLE)
        orders_o_orderkey = TableDefinition.Column('o_orderkey', SqlType.int(), nullability=NOT_NULLABLE)
        orders_o_custkey = TableDefinition.Column('o_custkey', SqlType.int(), nullability=NOT_NULLABLE)
        lineitem_l_orderkey = TableDefinition.Column('l_orderkey', SqlType.int(), nullability=NOT_NULLABLE)
        lineitem_l_linenumber = TableDefinition.Column('l_linenumber', SqlType.int(), nullability=NOT_NULLABLE)
        lineitem_l_partkey = TableDefinition.Column('l_partkey', SqlType.int(), nullability=NOT_NULLABLE)
        lineitem_l_suppkey = TableDefinition.Column('l_suppkey', SqlType.int(), nullability=NOT_NULLABLE) 
        
        class TPCHTables(Enum):
            REGION = TableDefinition('region', [
                region_r_regionkey,
                TableDefinition.Column('r_name', SqlType.varchar(25), nullability=NOT_NULLABLE),
                TableDefinition.Column('r_comment', SqlType.varchar(152), nullability=NOT_NULLABLE)
            ])
            NATION = TableDefinition('nation', [
                nation_n_nationkey,
                TableDefinition.Column('n_name', SqlType.varchar(25), nullability=NOT_NULLABLE),
                nation_n_regionkey,
                TableDefinition.Column('n_comment', SqlType.varchar(152), nullability=NOT_NULLABLE)
            ])
            SUPPLIER = TableDefinition('supplier', [
                supplier_s_suppkey,
                TableDefinition.Column('s_name', SqlType.varchar(25), nullability=NOT_NULLABLE),
                TableDefinition.Column('s_address', SqlType.varchar(40), nullability=NOT_NULLABLE),
                supplier_s_nationkey,
                TableDefinition.Column('s_phone', SqlType.char(15), nullability=NOT_NULLABLE),
                TableDefinition.Column('s_acctbal', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('s_comment', SqlType.varchar(101), nullability=NOT_NULLABLE)
            ])
            CUSTOMER = TableDefinition('customer', [
                customer_c_custkey,
                TableDefinition.Column('c_name', SqlType.varchar(25), nullability=NOT_NULLABLE),
                TableDefinition.Column('c_address', SqlType.varchar(40), nullability=NOT_NULLABLE),
                customer_c_nationkey,
                TableDefinition.Column('c_phone', SqlType.varchar(15), nullability=NOT_NULLABLE),
                TableDefinition.Column('c_acctbal', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('c_mktsegment', SqlType.varchar(10), nullability=NOT_NULLABLE),
                TableDefinition.Column('c_comment', SqlType.varchar(117), nullability=NOT_NULLABLE)
            ])
            PART = TableDefinition('part', [
                part_p_partkey,
                TableDefinition.Column('p_name', SqlType.varchar(55), nullability=NOT_NULLABLE),
                TableDefinition.Column('p_mfgr', SqlType.varchar(25), nullability=NOT_NULLABLE),
                TableDefinition.Column('p_brand', SqlType.varchar(10), nullability=NOT_NULLABLE),
                TableDefinition.Column('p_type', SqlType.varchar(25), nullability=NOT_NULLABLE),
                TableDefinition.Column('p_size', SqlType.int()),
                TableDefinition.Column('p_container', SqlType.varchar(10), nullability=NOT_NULLABLE),
                TableDefinition.Column('p_retailprice', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('p_comment', SqlType.varchar(23), nullability=NOT_NULLABLE),
            ])
            PARTSUPP = TableDefinition('partsupp', [
                partsupp_ps_partkey,
                partsupp_ps_suppkey,
                TableDefinition.Column('ps_availqty', SqlType.int(), nullability=NOT_NULLABLE),
                TableDefinition.Column('ps_supplycost', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('ps_comment', SqlType.varchar(199), nullability=NOT_NULLABLE)
            ])
            ORDERS = TableDefinition('orders', [
                orders_o_orderkey,
                orders_o_custkey,
                TableDefinition.Column('o_orderstatus', SqlType.varchar(1), nullability=NOT_NULLABLE),
                TableDefinition.Column('o_totalprice', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('o_orderdate', SqlType.date(), nullability=NOT_NULLABLE),
                TableDefinition.Column('o_orderpriority', SqlType.varchar(15), nullability=NOT_NULLABLE),
                TableDefinition.Column('o_clerk', SqlType.varchar(15), nullability=NOT_NULLABLE),
                TableDefinition.Column('o_shippriority', SqlType.int(), nullability=NOT_NULLABLE),
                TableDefinition.Column('o_comment', SqlType.varchar(79), nullability=NOT_NULLABLE)
            ])
            LINEITEM = TableDefinition('lineitem', [
                lineitem_l_orderkey,
                lineitem_l_partkey,
                lineitem_l_suppkey,
                lineitem_l_linenumber,
                TableDefinition.Column('l_quantity', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_extendedprice', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_discount', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_tax', SqlType.double(), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_returnflag', SqlType.varchar(1), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_linestatus', SqlType.varchar(1), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_shipdate', SqlType.date(), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_commitdate', SqlType.date(), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_receiptdate', SqlType.date(), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_shipinstruct', SqlType.varchar(25), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_shipmode', SqlType.varchar(10), nullability=NOT_NULLABLE),
                TableDefinition.Column('l_comment', SqlType.varchar(44), nullability=NOT_NULLABLE)
            ])
        
        class TPCHPrimaryKeys(Enum):
            REGION = PrimaryKey(TPCHTables.REGION.value.table_name, [region_r_regionkey.name])
            NATION = PrimaryKey(TPCHTables.NATION.value.table_name, [nation_n_nationkey.name])
            SUPPLIER = PrimaryKey(TPCHTables.SUPPLIER.value.table_name, [supplier_s_suppkey.name])
            CUSTOMER = PrimaryKey(TPCHTables.CUSTOMER.value.table_name, [customer_c_custkey.name])
            PART = PrimaryKey(TPCHTables.PART.value.table_name, [part_p_partkey.name])
            PARTSUPP = PrimaryKey(TPCHTables.PARTSUPP.value.table_name, [partsupp_ps_partkey.name, partsupp_ps_suppkey.name])
            ORDERS = PrimaryKey(TPCHTables.ORDERS.value.table_name, [orders_o_orderkey.name])
            LINEITEM = PrimaryKey(TPCHTables.LINEITEM.value.table_name, [lineitem_l_orderkey.name, lineitem_l_linenumber.name])
        
        class TPCHForeignKeys(Enum):
            NATION = ForeignKey(TPCHTables.NATION.value.table_name,
                                [nation_n_regionkey.name],
                                TPCHTables.REGION.value.table_name,
                                [region_r_regionkey.name])
            SUPPLIER = ForeignKey(TPCHTables.SUPPLIER.value.table_name,
                                  [supplier_s_nationkey.name],
                                  TPCHTables.NATION.value.table_name,
                                  [nation_n_nationkey.name])
            CUSTOMER = ForeignKey(TPCHTables.CUSTOMER.value.table_name,
                        [customer_c_nationkey.name],
                        TPCHTables.NATION.value.table_name,
                        [nation_n_nationkey.name])
            PARTSUPP_1 = ForeignKey(TPCHTables.PARTSUPP.value.table_name,
                                    [partsupp_ps_partkey.name],
                                    TPCHTables.PART.value.table_name,
                                    [part_p_partkey.name])
            PARTSUPP_2 = ForeignKey(TPCHTables.PARTSUPP.value.table_name,
                                    [partsupp_ps_suppkey.name],
                                    TPCHTables.SUPPLIER.value.table_name,
                                    [supplier_s_suppkey.name])
            ORDERS = ForeignKey(TPCHTables.ORDERS.value.table_name,
                                [orders_o_custkey.name],
                                TPCHTables.CUSTOMER.value.table_name,
                                [customer_c_custkey.name])
            LINEITEM_1 = ForeignKey(TPCHTables.LINEITEM.value.table_name,
                                    [lineitem_l_orderkey.name],
                                    TPCHTables.ORDERS.value.table_name,
                                    [orders_o_orderkey.name])
            LINEITEM_2 = ForeignKey(TPCHTables.LINEITEM.value.table_name,
                                    [lineitem_l_partkey.name, lineitem_l_suppkey.name],
                                    TPCHTables.PARTSUPP.value.table_name,
                                    [partsupp_ps_partkey.name, partsupp_ps_suppkey.name])
        
        # Set the Created objects as class attributes
        self.TPCHTables = TPCHTables
        self.TPCHPrimaryKeys = TPCHPrimaryKeys
        self.TPCHForeignKeys = TPCHForeignKeys
        
    def prepare_database(self, data_dir, constants_dir=None):
        # Create TableDefinitions
        self.__create_tpch_table_definitions()
        
        # Create the database, and replace if exists
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=self.hyper_parameters) as hyper:
            with Connection(hyper.endpoint, self.connection_details, CreateMode.CREATE_AND_REPLACE) as connection:
                assert connection.is_open and connection.is_ready == True
                print("Created new Database file, at: " + str(self.connection_details))

                # Now populate it with tables and data
                for table in self.TPCHTables:
                    connection.catalog.create_table(table_definition=table.value)
                print("Created Table Definitions")
                
                # Create 'assumed' primary key
                for primary_key in self.TPCHPrimaryKeys:
                    connection.execute_command(f'ALTER TABLE {primary_key.value.table_name} ADD ASSUMED PRIMARY KEY { primary_key.value.primary_keys }')
                print("Created Primary Keys")
                
                # Create 'assumed' foreign key
                for foreign_key in self.TPCHForeignKeys:
                    connection.execute_command(f'''ALTER TABLE { foreign_key.value.table_name } ADD ASSUMED FOREIGN KEY 
                    { foreign_key.value.table_keys } REFERENCES  { foreign_key.value.ref_table_name } { foreign_key.value.ref_table_keys }''')
                print("Created Foreign Keys")
                
                # Load data
                for table in self.TPCHTables:
                    file_path = f'{ str(data_dir) }/{ table.value.table_name.name.unescaped }.tbl.csv'
                    connection.execute_command(f"COPY { table.value.table_name } FROM '{ file_path }' ( FORMAT => 'csv', DELIMITER => '|' )")
                print("Loaded data into Tables")
            
        assert self.is_database_empty() == False
        
        print("HyperDB Database creation completed")
