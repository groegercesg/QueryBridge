from prepare_databases.prepare_database import PrepareDatabase
from tableauhyperapi import HyperProcess, Telemetry, CreateMode, Connection, TableDefinition, SqlType
from enum import Enum

class PrimaryKey():
    def __init__(self, table_name, primary_keys):
        self.table_name = table_name
        self.primary_keys = self.__process_keys(primary_keys)
    
    def __process_keys(self, keys):
        assert isinstance(keys, list), f"Unexpected primary_keys list format. It was of type: { type(keys) }"
        formatted_keys = [str(e) for e in keys]
        print(formatted_keys)
        return "(" + ', '.join(formatted_keys) + ")"
        
class ForeignKey():
    def __init__(self, table, keys, ref_table, ref_keys):
        assert len(keys) == len(ref_keys), "Foreign key references must have the same amount of keys."
        self.table_name = table
        self.table_keys = self.__process_keys(keys)
        self.ref_table_name = ref_table
        self.ref_table_keys = self.__process_keys(ref_keys)
    
    def __process_keys(self, keys):
        assert isinstance(keys, list), f"Unexpected primary_keys list format. It was of type: { type(keys) }"
        formatted_keys = [f'"{e}"' for e in keys]
        return f'({ ", ".join(formatted_keys) })'

class PrepareHyperDB(PrepareDatabase):
    def __init__(self, connection_details):
        super().__init__(connection_details, "Hyper DB")
        self.explain_options = "EXPLAIN (VERBOSE)"
        self.hyper_parameters = {
            #"log_config": "",
            "max_query_size": "10000000000",
            "hard_concurrent_query_thread_limit": "1"
        }
        
    def __open_connection(self):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=self.hyper_parameters) as hyper:
            return Connection(hyper.endpoint, self.connection_details, CreateMode.NONE)
    
    def is_database_empty(self):
        tables = []
        with self.__open_connection() as connection:
            catalog = connection.catalog
            schemas = catalog.get_schema_names()
            for schema_name in schemas:
                tables.extend(catalog.get_table_names(schema_name))
        if len(tables) > 0:
            return False
        else:
            print(tables)
            return True
    
    def execute_query(self, query_text):
        with self.__open_connection() as connection:
            return connection.execute_list_query(query_text)
    
    def get_explain(self, query_text, query_name=None):
        query_text = self.create_explain(query_text)
        return self.execute_query(query_text), query_text
    
    def __create_tpch_table_definitions(self):
        # Keys that we need to reference in many places
        region_r_regionkey = TableDefinition.Column('r_regionkey', SqlType.int())
        nation_n_nationkey = TableDefinition.Column('n_nationkey', SqlType.int())
        nation_n_regionkey = TableDefinition.Column('n_regionkey', SqlType.int())
        supplier_s_suppkey = TableDefinition.Column('s_suppkey', SqlType.int())
        supplier_s_nationkey = TableDefinition.Column('s_nationkey', SqlType.int())
        customer_c_custkey = TableDefinition.Column('c_custkey', SqlType.int())
        customer_c_nationkey = TableDefinition.Column('c_nationkey', SqlType.int())
        part_p_partkey = TableDefinition.Column('p_partkey', SqlType.int())
        partsupp_ps_partkey = TableDefinition.Column('ps_partkey', SqlType.int())
        partsupp_ps_suppkey = TableDefinition.Column('ps_suppkey', SqlType.int())
        orders_o_orderkey = TableDefinition.Column('o_orderkey', SqlType.int())
        orders_o_custkey = TableDefinition.Column('o_custkey', SqlType.int())
        lineitem_l_orderkey = TableDefinition.Column('l_orderkey', SqlType.int())
        lineitem_l_linenumber = TableDefinition.Column('l_linenumber', SqlType.int())
        lineitem_l_partkey = TableDefinition.Column('l_partkey', SqlType.int())
        lineitem_l_suppkey = TableDefinition.Column('l_suppkey', SqlType.int()) 
        
        class TPCHTables(Enum):
            REGION = TableDefinition('region', [
                region_r_regionkey,
                TableDefinition.Column('r_name', SqlType.varchar(25)),
                TableDefinition.Column('r_comment', SqlType.varchar(152))
            ])
            NATION = TableDefinition('nation', [
                nation_n_nationkey,
                TableDefinition.Column('n_name', SqlType.varchar(25)),
                nation_n_regionkey,
                TableDefinition.Column('n_comment', SqlType.varchar(152))
            ])
            SUPPLIER = TableDefinition('supplier', [
                supplier_s_suppkey,
                TableDefinition.Column('s_name', SqlType.varchar(25)),
                TableDefinition.Column('s_address', SqlType.varchar(40)),
                supplier_s_nationkey,
                TableDefinition.Column('s_phone', SqlType.char(15)),
                TableDefinition.Column('s_acctbal', SqlType.double()),
                TableDefinition.Column('s_comment', SqlType.varchar(101))
            ])
            CUSTOMER = TableDefinition('customer', [
                customer_c_custkey,
                TableDefinition.Column('c_name', SqlType.varchar(25)),
                TableDefinition.Column('c_address', SqlType.varchar(40)),
                customer_c_nationkey,
                TableDefinition.Column('c_phone', SqlType.varchar(15)),
                TableDefinition.Column('c_acctbal', SqlType.double()),
                TableDefinition.Column('c_mktsegment', SqlType.varchar(10)),
                TableDefinition.Column('c_comment', SqlType.varchar(117))
            ])
            PART = TableDefinition('part', [
                part_p_partkey,
                TableDefinition.Column('p_name', SqlType.varchar(55)),
                TableDefinition.Column('p_mfgr', SqlType.varchar(25)),
                TableDefinition.Column('p_brand', SqlType.varchar(10)),
                TableDefinition.Column('p_type', SqlType.varchar(25)),
                TableDefinition.Column('p_size', SqlType.int()),
                TableDefinition.Column('p_container', SqlType.varchar(10)),
                TableDefinition.Column('p_retailprice', SqlType.double()),
                TableDefinition.Column('p_comment', SqlType.varchar(23)),
            ])
            PARTSUPP = TableDefinition('partsupp', [
                partsupp_ps_partkey,
                partsupp_ps_suppkey,
                TableDefinition.Column('ps_availqty', SqlType.int()),
                TableDefinition.Column('ps_supplycost', SqlType.double()),
                TableDefinition.Column('ps_coment', SqlType.varchar(199))
            ])
            ORDERS = TableDefinition('orders', [
                orders_o_orderkey,
                orders_o_custkey,
                TableDefinition.Column('o_orderstatus', SqlType.varchar(1)),
                TableDefinition.Column('o_totalprice', SqlType.double()),
                TableDefinition.Column('o_orderdate', SqlType.date()),
                TableDefinition.Column('o_orderpriority', SqlType.varchar(15)),
                TableDefinition.Column('o_clerk', SqlType.varchar(15)),
                TableDefinition.Column('o_shippriority', SqlType.int()),
                TableDefinition.Column('o_comment', SqlType.varchar(79))
            ])
            LINEITEM = TableDefinition('lineitem', [
                lineitem_l_orderkey,
                lineitem_l_partkey,
                lineitem_l_suppkey,
                lineitem_l_linenumber,
                TableDefinition.Column('l_quantity', SqlType.double()),
                TableDefinition.Column('l_extendedprice', SqlType.double()),
                TableDefinition.Column('l_discount', SqlType.double()),
                TableDefinition.Column('l_tax', SqlType.double()),
                TableDefinition.Column('l_returnflag', SqlType.varchar(1)),
                TableDefinition.Column('l_linestatus', SqlType.varchar(1)),
                TableDefinition.Column('l_shipdate', SqlType.date()),
                TableDefinition.Column('l_commitdate', SqlType.date()),
                TableDefinition.Column('l_receiptdate', SqlType.date()),
                TableDefinition.Column('l_shipinstruct', SqlType.varchar(25)),
                TableDefinition.Column('l_shipmode', SqlType.varchar(10)),
                TableDefinition.Column('l_comment', SqlType.varchar(44))
            ])
        
        class TPCHPrimaryKeys(Enum):
            REGION = PrimaryKey(TPCHTables.REGION.value.table_name, [region_r_regionkey.name])
            NATION = PrimaryKey(TPCHTables.NATION.value.table_name, [nation_n_nationkey.name])
            SUPPLIER = PrimaryKey(TPCHTables.SUPPLIER.value.table_name, [supplier_s_suppkey.name])
            CUSTOMER = PrimaryKey(TPCHTables.CUSTOMER.value.table_name, [customer_c_custkey.name])
            PART = PrimaryKey(TPCHTables.PART.value.table_name, [part_p_partkey.name])
            PARTSUPP = PrimaryKey(TPCHTables.PART.value.table_name, [partsupp_ps_partkey.name, partsupp_ps_suppkey.name])
            ORDERS = PrimaryKey(TPCHTables.ORDERS.value.table_name, [orders_o_orderkey.name])
            LINEITEM = PrimaryKey(TPCHTables.LINEITEM.value.table_name, [lineitem_l_orderkey.name, lineitem_l_linenumber.name])
        
        class TPCHForeignKeys(Enum):
            NATION = ForeignKey(TPCHTables.NATION.value.table_name,
                                [nation_n_regionkey],
                                TPCHTables.REGION.value.table_name,
                                [region_r_regionkey])
            SUPPLIER = ForeignKey(TPCHTables.SUPPLIER.value.table_name,
                                  [supplier_s_nationkey],
                                  TPCHTables.NATION.value.table_name,
                                  [nation_n_nationkey])
            CUSTOMER = ForeignKey(TPCHTables.CUSTOMER.value.table_name,
                        [customer_c_nationkey],
                        TPCHTables.CUSTOMER.value.table_name,
                        [nation_n_nationkey])
            PARTSUPP_1 = ForeignKey(TPCHTables.PARTSUPP.value.table_name,
                                    [partsupp_ps_partkey],
                                    TPCHTables.PART.value.table_name,
                                    [part_p_partkey])
            PARTSUPP_2 = ForeignKey(TPCHTables.PARTSUPP.value.table_name,
                                    [partsupp_ps_suppkey],
                                    TPCHTables.SUPPLIER.value.table_name,
                                    [supplier_s_suppkey])
            ORDERS = ForeignKey(TPCHTables.ORDERS.value.table_name,
                                [orders_o_custkey],
                                TPCHTables.CUSTOMER.value.table_name,
                                [customer_c_custkey])
            LINEITEM_1 = ForeignKey(TPCHTables.LINEITEM.value.table_name,
                                    [lineitem_l_orderkey],
                                    TPCHTables.ORDERS.value.table_name,
                                    [orders_o_orderkey])
            LINEITEM_2 = ForeignKey(TPCHTables.LINEITEM.value.table_name,
                                    [lineitem_l_partkey, lineitem_l_suppkey],
                                    TPCHTables.PARTSUPP.value.table_name,
                                    [partsupp_ps_partkey, partsupp_ps_suppkey])
        
        
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

                print(connection.is_open)
                print(connection.is_ready)
                # Now populate it with tables and data
                for table in self.TPCHTables:
                    print(f"Creating Table: {table.value.table_name}")
                    connection.catalog.create_table(table_definition=table.value)
                print("Created tables")
                
                # Create 'assumed' primary key
                for primary_key in self.TPCHPrimaryKeys:
                    connection.execute_command(f'ALTER TABLE {primary_key.value.table_name} ADD ASSUMED PRIMARY KEY { primary_key.value.primary_keys }')
                print("Created Primary Keys")
                
                # Create 'assumed' foreign key
                for foreign_key in self.TPCHForeignKeys:
                    connection.execute_command(f'''ALTER TABLE { foreign_key.value.table_name } ADD ASSUMED FOREIGN KEY 
                    { foreign_key.table_keys } REFERENCES  { foreign_key.value.ref_table_name } { foreign_key.value.ref_table_keys }''')
                print("Created Foreign Keys")
                
                # Load data
                for table in self.TPCHTables:
                    connection.execute_command(f"COPY { table.value.table_name } FROM '{ f'{ str(data_dir) }/{ table.value.table_name }.tbl.csv' }' ( FORMAT => 'csv', DELIMITER => '|' )")
                print("Loaded data into tables")
            
        assert self.is_database_empty() == False
        
        print("HyperDB Database creation completed")
