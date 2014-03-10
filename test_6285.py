from dtest import Tester, debug
from tools import ThriftConnection
import time
import datetime
import uuid
import os
import pycassa
import threading

JNA_PATH = '/usr/share/java/jna/jna.jar'

class CompactionTimer(threading.Thread):
    def __init__(self, node, wait_seconds):
        self.wait_seconds = wait_seconds
        self.node = node
        threading.Thread.__init__(self)

    def run(self):
        time.sleep(self.wait_seconds)
        debug("Compacting...")
        self.node.compact()


class RowWriter(threading.Thread):
    def __init__(self, num_rows):
        self.num_rows = num_rows
        threading.Thread.__init__(self)
        
    def run(self):
        pool = pycassa.ConnectionPool('test')
        cf = pycassa.ColumnFamily(pool, 'CF')
        debug("Thread %s starting write of %d rows..." % (threading.current_thread(), self.num_rows))
        for x in xrange(self.num_rows):
            cf.insert(str(x), {datetime.datetime.utcnow(): 'blah'})
        debug("Thread %s done writing" % (threading.current_thread(),))
        


class Test6285(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def test_6285(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={ 'rpc_server_type' : 'hsha'})

        # Enable JNA:
        with open(os.path.join(self.test_path, 'test', 'cassandra.in.sh'),'w') as f:
            f.write('CLASSPATH={jna_path}:$CLASSPATH\n'.format(
                jna_path=JNA_PATH))

        cluster.populate(2)
        cluster.start(use_jna=True)

        (node1, node2) = cluster.nodelist()


        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'test', 2)

        cursor.execute("""CREATE TABLE "CF" (
  key blob,
  column1 timeuuid,
  value blob,
  PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=7200 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'chunk_length_kb': '64', 'sstable_compression': 'DeflateCompressor'};
""")


        threads = [RowWriter(100000) for r in range(10)]
        for t in threads:
            t.start()
        compactors = [CompactionTimer(node1, 10),
                      CompactionTimer(node2, 12),
                      CompactionTimer(node1, 30),
                      CompactionTimer(node2, 32)]
        for t in compactors:
            t.start()

        for t in threads:
            t.join()
        for t in compactors:
            t.join()
        
        node1.compact()
        node1.stop()
        node1.start()

