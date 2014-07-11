from dtest import Tester, debug
import time
from tools import *
# import os, sys, time, tools
# from uuid import UUID
# from ccmlib.cluster import Cluster

from winpexpect import *
from ccmlib import common

def assert_columns(cli, names):
    # assert not cli.has_errors(), cli.errors()
    output = cli.last_output()

    for name in names:
        assert re.search('name=%s' % name, output) is not None, 'Cannot find column %s in %s' % (name, output)

class TestSCCache(Tester):

    def sc_with_row_cache_test(self):
    
        debug("Starting cluster...")
        cluster = self.cluster
        cluster.populate(1).start()
        
        debug("Version: " + cluster.version())
        time.sleep(3)
        
        debug("Getting Thrift CLI...")
        [node1] = cluster.nodelist()
        
        cdir = cluster.get_cassandra_dir()
        cli = common.join_bin(cdir, 'bin', 'cassandra-cli')
        env = common.make_cassandra_env(cdir, node1.get_path())
            
        child = winspawn(cli)
        # debug(str(child))
        child.logfile = sys.stdout
        
        child.expect("Type .*")
        child.write("create keyspace ks;")
        
        child.expect(".*");
        child.write("use ks;")

        child.expect("Authenticated to keyspace: ks")
        
        child.kill(9)


        
        self.assertEqual(1,2)

    # def sc_with_row_cache_test(self):
        # """ Test for bug reported in #4190 """
        # cluster = self.cluster

        # cluster.populate(1).start()
        # node1 = cluster.nodelist()[0]
        # time.sleep(0.2)

        # cli = node1.cli()
        # cli.do("something")
        # cli.do("create keyspace ks")
        # cli.do("use ks")
        # if cluster.version() < "1.1":
            # cli.do("""
               # create column family Users
               # with column_type='Super' and key_validation_class='UTF8Type' and comparator='UTF8Type' and subcomparator='UTF8Type' and default_validation_class='UTF8Type'
               # and rows_cached=75000 and row_cache_provider='ConcurrentLinkedHashCacheProvider';
            # """)
        # else:
            # cli.do("""
               # create column family Users
               # with column_type='Super' and key_validation_class='UTF8Type' and comparator='UTF8Type' and subcomparator='UTF8Type' and default_validation_class='UTF8Type'
               # and caching='ROWS_ONLY';
            # """)
        
        # cli.do("set Users['mina']['attrs']['name'] = 'Mina'")
        # cli.do("get Users['mina']")
        # assert_columns(cli, ['name'])

        # cli.do("set Users['mina']['attrs']['country'] = 'Canada'")
        # cli.do("get Users['mina']")
        # assert_columns(cli, ['name', 'country'])

        # cli.do("set Users['mina']['attrs']['region'] = 'Quebec'")
        # cli.do("get Users['mina']")
        # assert_columns(cli, ['name', 'country', 'region'])
