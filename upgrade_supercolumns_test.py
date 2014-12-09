from dtest import Tester, debug
from ccmlib.cluster import Cluster
from ccmlib.common import get_version_from_build
from pytools import since
import random, os, time, re

# Tests upgrade between 1.2->2.0 for super columns (since that's where
# we removed then internally)
class TestSCUpgrade(Tester):

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def slice_query_test(self):
        cluster = self.cluster

        cluster.set_install_dir(version="1.2.19")
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()

        cli = node1.cli()
        cli.do("create keyspace test with placement_strategy = 'SimpleStrategy' and strategy_options = {replication_factor : 2} and durable_writes = true")
        cli.do("use test")
        cli.do("create column family sc_test with column_type = 'Super' and default_validation_class = 'CounterColumnType' AND key_validation_class=UTF8Type AND comparator=IntegerType AND subcomparator=UTF8Type")

        assert not cli.has_errors(), cli.errors()

        for i in xrange(40):
            for k in xrange(random.randint(0,15)):
                cli.do("incr sc_test['Counter1'][-12]['%d'] by 1" % i)

        assert not cli.has_errors(), cli.errors()
        cli.close()

        self.upgrade_to_version("git:cassandra-2.0")

        session = self.cql_connection(node1)
        session.execute("Use test")
        rows = session.execute("Select * from sc_test where key='Counter1' limit 10")
        slice_rows = session.execute("Select * from sc_test where key='Counter1' and column1 = -12 limit 10")
        assert rows == slice_rows

    def upgrade_to_version(self, tag, nodes=None):
        debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
