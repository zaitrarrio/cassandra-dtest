from dtest import Tester, debug
from tools import ThriftConnection
import time
import datetime
import uuid
import os
import pycassa
import threading
import random

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
    def __init__(self, row_start, row_stop):
        self.row_start = row_start
        self.row_stop = row_stop
        self.num_rows = row_stop - row_start - 1
        threading.Thread.__init__(self)
        
    def run(self):
        pool = pycassa.ConnectionPool('test')
        cf = pycassa.ColumnFamily(pool, 'CF')
        debug("Thread %s starting write of %d rows..." % (threading.current_thread(), self.num_rows))
        for x in xrange(self.row_start, self.row_stop):
            cf.insert(str(x), {datetime.datetime.utcnow(): 'blah'})
        debug("Thread %s done writing" % (threading.current_thread(),))
        

class RowDeleter(threading.Thread):
    def __init__(self, row_keys, delay_between_deletes=0):
        self.row_keys = row_keys
        self.delay_between_deletes = delay_between_deletes
        threading.Thread.__init__(self)
        
    def run(self):
        pool = pycassa.ConnectionPool('test')
        cf = pycassa.ColumnFamily(pool, 'CF')
        debug("Thread %s starting deletes ..." % (threading.current_thread(),))
        for x in self.row_keys:
            time.sleep(self.delay_between_deletes)
            cf.remove(str(x))
        debug("Thread %s done deleting" % (threading.current_thread(),))


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
  compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 2} AND
  compression={'chunk_length_kb': '64', 'sstable_compression': 'DeflateCompressor'};
""")


        def start_writes(num_threads):
            write_threads = []
            rows_per_thread = 1000
            rows = 0
            for t in range(num_threads):
                t = RowWriter(rows, rows+rows_per_thread)
                write_threads.append(t)
                rows += rows_per_thread
                t.start()

            return write_threads, rows

        def start_deletes(num_threads, max_row):
            delete_threads = []
            for t in range(num_threads):
                  t = RowDeleter([random.randint(0,max_row) for x in xrange(100)])
                  delete_threads.append(t)
                  t.start()

            return delete_threads


        # Do an initial big write:
        write_threads, first_write_rows = start_writes(50)
        # Delete from the first write at the same time:
        delete_threads = start_deletes(10, first_write_rows)
        
        for t in write_threads:
            t.join()
        for t in delete_threads:
            t.join()

        cluster.compact()

        node2.stop()
        node2.start()
        node1.stop()
        node1.start()
