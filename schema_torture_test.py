import time
import os
import pprint
from threading import Thread

from dtest import Tester, debug
from ccmlib.cluster import Cluster
from ccmlib.node import Node
from tools import since

import cql
import pdb

class ConcurrentQuery(Thread):
    def __init__(self, query, cursor):
        Thread.__init__(self)
        self.query = query
        self.cursor = cursor
        
    def run(self):
        self.cursor.execute("USE ks;")
        self.cursor.execute( self.query )


class SchemaTortureTest(Tester):
    def __init__(self, *argv, **kwargs):
        super(SchemaTortureTest, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True

                
    def _check_num_tables(self, node, num_tables):
        cursor = self.cql_connection(node).cursor()
        cursor.execute("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='ks'")
        res = cursor.fetchall()
        debug("number of tables counted: %s" % str(len(res)))
        assert len(res) == num_tables, "Schema disagrement."

    def cycle_create_drop_test(self):
        cluster_size = 6
        replication_factor = 3
        num_tables = 6
        num_cycles = 5
        
        cluster = self.cluster
        cluster.populate( cluster_size ).start()
        
        nodes = cluster.nodelist()
        
        node1 = nodes[0]
        
        cursor = self.cql_connection(node1).cursor()

        self.create_ks(cursor, "ks", cluster_size / 2)
        
        cursor.execute('USE ks;')

        create_schema_template = ("CREATE TABLE test{table_num} ( "
                                  "row text, "
                                  "val1 int, "
                                  "val2 varchar, "
                                  "PRIMARY KEY (row));")

        delete_schema_template = ("DROP TABLE test{table_num};")

        for cycle in range(num_cycles):
            debug("starting cycle %s" % str(cycle) )
            create_table_queries = [ ConcurrentQuery( create_schema_template.format(table_num=i),
                                                    self.cql_connection(node1).cursor() ) for i in range( num_tables ) ]
            debug("Creating %s tables" % str(num_tables))
            for query in create_table_queries:
                query.start()
            for query in create_table_queries:
                query.join()
            time.sleep(1.0)
            #ensure the correct number of tables were created and all nodes agree
            for node in nodes:
                self._check_num_tables(node, num_tables)
                        
            delete_table_quries = [ ConcurrentQuery( delete_schema_template.format(table_num=i),
                                                     self.cql_connection(node1).cursor() ) for i in range( num_tables ) ]

            debug("Deleting tables.")
            for query in delete_table_quries:
                query.start()
            for query in delete_table_quries:
                query.join()
            time.sleep(1.0)
            for node in nodes:
                self._check_num_tables(node, 0)
        