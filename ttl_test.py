from dtest import Tester, debug
from pytools import since, rows_to_list
import time


class TestTTL(Tester):
    """ Test Time To Live Feature """

    def setUp(self):
        Tester.setUp(self)
        self.cluster.populate(3).start()
        [node1,node2, node3] = self.cluster.nodelist()
        self.cursor1 = self.patient_cql_connection(node1)
        self.create_ks(self.cursor1, 'ks', 3)

    def prepare(self, default_time_to_live=None):
        self.cursor1.execute("DROP TABLE IF EXISTS ttl_table;")
        query = """
            CREATE TABLE ttl_table (
                key int primary key,
                col1 int,
                col2 int,
                col3 int,
            )
        """
        if default_time_to_live:
            query += " WITH default_time_to_live = {}".format(default_time_to_live);

        self.cursor1.execute(query)

    def check_num_rows(self, expected):
        """ Function to validate the number of rows expected in the table """

        query = "SELECT count(*) FROM ttl_table;"
        result = self.cursor1.execute(query)
        count = result[0][0]
        self.assertEqual(
            count,
            expected,
            "Expected %d results from '%s', but got %d" % (expected, query, count)
        )

    def check_rows(self, expected):
        """ Function to validate the rows data """

        results = self.cursor1.execute("SELECT * FROM ttl_table;")
        self.assertEqual(rows_to_list(results), expected, results)

    def safe_sleep(self, start_time, time_to_wait):
        """ Function that simulates a sleep, but no longer than the desired time_to_wait.
            Useful when tests are slower than expected.

            start_time: The start time of the timed operations
            time_to_wait: The time to wait in seconds from the start_time
        """

        now = time.time()
        real_time_to_wait = time_to_wait - (now - start_time)

        if real_time_to_wait > 0:
            time.sleep(real_time_to_wait)

    @since('2.0')
    def default_ttl_test(self):
        """ Test default_time_to_live specified on a table """

        self.prepare(default_time_to_live=1)
        start = time.time()
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (1, 1))
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (2, 2))
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (3, 3))
        self.safe_sleep(start, 1.5)
        self.check_num_rows(0)

    @since('2.0')
    def insert_ttl_has_priority_on_defaut_ttl_test(self):
        """ Test that a ttl specified during an insert has priority on the default table ttl """

        self.prepare(default_time_to_live=1)

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 2;
        """ % (1, 1))
        self.safe_sleep(start, 1)
        self.check_num_rows(1)  # should still exist
        self.safe_sleep(start, 2.5)
        self.check_num_rows(0)

    @since('2.0')
    def insert_ttl_works_without_defaut_ttl_test(self):
        """ Test that a ttl specified during an insert works even if a table has no default ttl """

        self.prepare()

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 1;
        """ % (1, 1))
        self.safe_sleep(start, 1.5)
        self.check_num_rows(0)

    @since('2.0')
    def default_ttl_can_be_removed_test(self):
        """ Test that default_time_to_live can be removed """

        self.prepare(default_time_to_live=1)

        start = time.time()
        self.cursor1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.safe_sleep(start, 1.5)
        self.check_num_rows(1)

    @since('2.0')
    def removing_default_ttl_does_not_affect_existing_rows_test(self):
        """ Test that removing a default_time_to_live doesn't affect the existings rows """

        self.prepare(default_time_to_live=1)

        self.cursor1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 3;")
        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 5;
        """ % (2, 1))
        self.cursor1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d);" % (3, 1))
        self.safe_sleep(start, 1)
        self.check_num_rows(3)
        self.safe_sleep(start, 3)
        self.check_num_rows(2)
        self.safe_sleep(start, 5)
        self.check_num_rows(1)

    @since('2.0')
    def update_single_column_ttl_test(self):
        """ Test that specifying a TTL on a single column works """

        self.prepare()

        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (1, 1, 1, 1))
        start = time.time()
        self.cursor1.execute("UPDATE ttl_table USING TTL 2 set col1=42 where key=%s;" % (1,))
        self.check_rows([[1, 42, 1, 1]])
        self.safe_sleep(start, 2.5)
        self.check_rows([[1, None, 1, 1]])

    @since('2.0')
    def update_multiple_columns_ttl_test(self):
        """ Test that specifying a TTL on multiple columns works """

        self.prepare()

        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (1, 1, 1, 1))
        start = time.time()
        self.cursor1.execute("""
            UPDATE ttl_table USING TTL 2 set col1=42, col2=42, col3=42 where key=%s;
        """ % (1,))
        self.check_rows([[1, 42, 42, 42]])
        self.safe_sleep(start, 2.5)
        self.check_rows([[1, None, None, None]])

    @since('2.0')
    def update_column_ttl_with_row_ttl_test(self):
        """
        Test that specifying a column ttl works when a row ttl is set.
        This test specify a lower ttl for the column than the row ttl.
        """

        self.prepare()

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d) USING TTL 3;
        """  % (1, 1, 1, 1))
        self.cursor1.execute("UPDATE ttl_table USING TTL 1 set col1=42 where key=%s;" % (1,))
        self.check_rows([[1, 42, 1, 1]])
        self.safe_sleep(start, 2)
        self.check_rows([[1, None, 1, 1]])
        self.safe_sleep(start, 3)
        self.check_num_rows(0)

    @since('2.0')
    def update_column_ttl_with_row_ttl_test2(self):
        """
        Test that specifying a column ttl works when a row ttl is set.
        This test specify a higher column ttl than the row ttl.
        """

        self.prepare()

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d) USING TTL 2;
        """  % (1, 1, 1, 1))
        self.cursor1.execute("UPDATE ttl_table USING TTL 3 set col1=42 where key=%s;" % (1,))
        self.safe_sleep(start, 2)
        self.check_rows([[1, 42, 1, 1]])
        self.safe_sleep(start, 3)
        self.check_num_rows(0)
