from dtest import Tester
from assertions import *
from tools import *

import re, time

class TestCounterBug(Tester):

    @since("1.1") # This bug has always been there but the test use the ALTER WITH syntax introduced in 1.1
    def counter_bug_test(self):
        """ Test boostrap with counters at RF=1 """
        cluster = self.cluster

        cluster.populate(1, tokens=[0])
        node1 = cluster.nodelist()[0]
        node1.set_log_level("DEBUG")
        node1.start()
        time.sleep(.5)

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', validation="CounterColumnType")

        # Deactivate compaction to make tests more predictable
        cursor.execute("ALTER TABLE cf WITH max_compaction_threshold = 0")

        nb_counters = 10

        for i in xrange(1, nb_counters):
            cursor.execute("UPDATE cf SET x = x + 1 WHERE key=k%d" % i)

        node1.flush()

        for i in xrange(1, nb_counters):
            cursor.execute("UPDATE cf SET x = x + 1 WHERE key=k%d" % i)

        node1.flush()

        for i in xrange(1, nb_counters):
            cursor.execute("SELECT x FROM cf WHERE key=k%d" % i)
            assert cursor.fetchall() == [[2]], cursor.fetchall()

        node2 = new_node(cluster, token=2**126)
        node2.set_log_level("DEBUG")
        node2.start()
        time.sleep(1)

        for i in xrange(1, nb_counters):
            cursor.execute("SELECT x FROM cf WHERE key=k%d" % i)
            res = cursor.fetchall()
            assert res == [[2]], "for k%d: %s" % (i, str(res))
