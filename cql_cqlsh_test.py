from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
import random
import time
import pprint

from cqlsh_module import configuration_factory
from cqlsh_module.multi_connection import MultiConnection

"""
Environment Variables:
    CQL_VERSION
"""

class TestCqlCqlsh(Tester):

    def cql_cqlsh_test(self):

        cluster = self.cluster
        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(.2)

        host, port = node1.network_interfaces['thrift']
        conn = MultiConnection(host, port, node1.get_cassandra_dir())

        configuration_factory.iterate_over_configurations(conn)

