import os
import cql
import pexpect
import pprint
import re
import decimal
import time

from debug import debug

"""
Talks to cql
"""

CQL_VERSION = os.environ.get('CQL_VERSION', None)

# matches a single colored output of cqlsh data
CQLSH_OUTPUT_VALUE_RE = re.compile(r'\x1b\[(?:\d;\d;\d\dm|\d;\d\dm)(.*?)\x1b\[0m')

class MultiConnection(object):
    """
    Creates a connection to cassandra through both cql, and cqlsh.
    """
    def __init__(self, host, port, cassandra_dir=None, cql_version=None):
        # create the cql connection
        if cql_version:
            self.cql_cursor = cql.connect(host, port, cql_version=cql_version).cursor()
        else:
            self.cql_cursor = cql.connect(host, port).cursor()

        # Make sure to update this any time the prompt changes, like when "use"ing a keyspace.
        self.has_cqlsh = False
        if cassandra_dir:
            self.has_cqlsh = True
            self.cqlsh_prompt = 'cqlsh>'
            pe_cmd = "%s/bin/cqlsh %s %s" % (cassandra_dir, host, port)
            self.pe = pexpect.spawn(pe_cmd)
            self.pe.logfile = open('cqlsh_output.log', 'w')
            self.pe.expect(self.cqlsh_prompt)
            self.current_cqlsh_keyspace = None

        self.created_columnfamilies = set()
        self.current_cql_keyspace = None

    def create_ks(self, ks_name, options_str):
        """
        creates a keyspace and specifies that it should be used.
        does a "USE <keyspace>" as well.
        """

        # prepend the keyspaces so that cql and cqlsh can have different ones.
        cql_ks_name = ks_name
        cql_ks_name = cql_ks_name[:32] # keyspace names can't be longer then 32 chars
        out = None
        try:
            self.cql_cursor.execute('use ' + cql_ks_name)
        except cql.ProgrammingError, e:
            "Execution comes here if the keyspace doesn't exist"
            cql_str = "CREATE KEYSPACE %s WITH %s" % (cql_ks_name, options_str)
            self.cql_cursor.execute(cql_str)
            use_statement = 'use ' + cql_ks_name
            self.cql_cursor.execute(use_statement)
            out = [cql_str, use_statement]

        self.current_cql_keyspace = cql_ks_name

        # now make the keyspace in cqlsh
        if self.has_cqlsh:
            cqlsh_ks_name = ('cqlsh_' + ks_name)[:32]
            new_prompt = 'cqlsh:%s>' % cqlsh_ks_name
            use_statement = 'use %s;' % cqlsh_ks_name
            self.pe.sendline(use_statement)
            i = self.pe.expect([new_prompt, "Bad Request: Keyspace"], timeout=60)
            if i==1:
                # execution comes here if the keyspace doesn't exist
                cqlsh_str = "CREATE KEYSPACE %s WITH %s;" % (cqlsh_ks_name, options_str)
                self.pe.sendline(cqlsh_str)
                self.pe.expect(self.cqlsh_prompt, timeout=60)
                self.pe.sendline(use_statement)
                self.cqlsh_prompt = new_prompt
                self.pe.expect(self.cqlsh_prompt, timeout=60)

            self.current_cqlsh_keyspace = cqlsh_ks_name
                
            self.cqlsh_prompt = new_prompt

        return out

    def create_cf(self, query, cf_name):
        """
        similar to execute, but makes sure that a cf is not created more then once.
        """
        unique_str = self.current_cql_keyspace + ' ' + cf_name 
        if unique_str not in self.created_columnfamilies:
            self.execute(query)
            self.created_columnfamilies.add(unique_str)


    def execute(self, query):
        """
        executes the query through both CQL and CQLSH. Don't forget the ending semicolon!
        """
        debug(query)

        if query.find('|') != -1:
            raise Exception("The pipe character was found in the query. The pipe is used as a delimiter "
                    "in cqlsh, and these tests don't currently support it's use.")

        # run the query in cql
        for try_num in xrange(10):
            try:
                # run the statement in cql
                self.cql_cursor.execute(query)
                cql_results = self.cql_cursor.fetchall()
                break
            except cql.OperationalError, e:
                if try_num == 9:
                    raise
                # wait a bit, then try again
                time.sleep(1)

        # run the query in cqlsh.
        if self.has_cqlsh:
            for try_num in xrange(10):
                self.pe.sendline(query)
                i = self.pe.expect([self.cqlsh_prompt, "\x1b\[0;1;\d\dmRequest did not complete within rpc_timeout\.\x1b\[0m"], timeout=60)
                if i == 0:
                    break
                elif i == 1:
                    debug("CQLSH RPC Timeout!")
                    self.pe.expect(self.cqlsh_prompt, timeout=60)
                    if try_num == 9:
                        raise Exception("CQLSH timed out 10 times when trying to insert. query: %s"%query)
                    time.sleep(5)

        return cql_results

    def get_cqlsh_output(self):
        """
        returns a list of lines from cqlsh that contain output values
        """
        output_lines = []        
        cqlsh_lines = self.pe.before.splitlines()
        for line in cqlsh_lines:
            if CQLSH_OUTPUT_VALUE_RE.search(line) or line.find('|') != -1:
                output_lines.append(line)

        if re.search(r"\x1b\[0;1;31mcannot concatenate '[\w]+' and '[\w]+' objects\x1b\[0m", output_lines[0]):
            raise Exception('CQLSH ERROR: %s' % output_lines[0])

        return output_lines

    def select_static(self, query, expected_results):
        """
        cqlsh provides different output for dynamic vs static column families.
        This function is for static columnfamilies.
        """
        cql_results = self.execute(query)

        # make a list of lines that we care about.
        cqlsh_output_lines = self.get_cqlsh_output()

        # names of columns. Currently ignored.
        col_names = CQLSH_OUTPUT_VALUE_RE.findall(cqlsh_output_lines[0])

        # put all the cqlsh output into a nice structure
        cqlsh_results = []
        for line in cqlsh_output_lines[1:]:
            slots = re.findall(r'[^\|]+', line) # a list of string slots where values should be.
            slot_id = 0
            col_list = []
            for slot in slots:
                found_in_slot = CQLSH_OUTPUT_VALUE_RE.findall(slot)
                assert len(found_in_slot) == 1, "Could not extract column from cqlsh output. Did the output format or appearance change? If so, update the CQLSH_OUTPUT_VALUE_RE regex to support the changes."
                col_value = found_in_slot[0]
                col_name = col_names[slot_id]
                col_list.append({'name': col_name, 'value': col_value})
                slot_id += 1
            cqlsh_results.append(col_list)

        self.verify_results(cql_results, cqlsh_results, expected_results)
    
    def select_dynamic(self, query, expected_results):
        """
        select from a dynamic columnfamily. Make sure the output is good.
        """
        cql_results = self.execute(query)

        cqlsh_output_lines = self.get_cqlsh_output()

        cqlsh_results = []
        for line in cqlsh_output_lines:
            slots = re.findall(r'[^\|]+', line)
            col_list = []
            for slot in slots:
                found_in_slot = CQLSH_OUTPUT_VALUE_RE.findall(slot)
                assert len(found_in_slot) == 2, "Could not extract column name and value from cqlsh output. Did the output format or appearance change? If so, update the CQLSH_OUTPUT_VALUE_RE regex to support the changes."
                col_name, col_value = found_in_slot
                col_list.append({'name': col_name, 'value': col_value})

            cqlsh_results.append(col_list)

        self.verify_results(cql_results, cqlsh_results, expected_results)
                


    def verify_results(self, cql_results, cqlsh_results, expected_results):

        row_index = 0
        for cql_row in cql_results: # loop through rows from cql
            cqlsh_row = cqlsh_results[row_index]
            col_index = 0
            for cql_val in cql_row: # loop through cols from cql
                cqlsh_val = cqlsh_row[col_index]['value']
                col_name = cqlsh_row[col_index]['name']

                if col_index == 0:
                    typ = expected_results[row_index]['key_type']
                    expected_val = expected_results[row_index]['key']
                else:
                    try:
                        expected_col = expected_results[row_index]['column_dict'][col_name]
                    except KeyError, e:
                        # columns with 'blob' as the validator have to be hex encoded to match. 
                        # Try treating the column as a blob if the first attempt didn't work.
                        expected_col = expected_results[row_index]['column_dict'][col_name.encode('hex')]
                    typ = expected_col['type']
                    expected_val = expected_col['value']

                err_msg = "A value from CQL and CQLSH did not match. CQL: '%s' CQLSH: '%s' expected: '%s'" % (str(cql_val), str(cqlsh_val), str(expected_val))
                if typ in ('ascii', 'text', 'varchar'):
                    assert unicode(cql_val) == unicode(cqlsh_val), err_msg
                    assert unicode(cql_val) == unicode(expected_val)
                elif typ in ('int', 'bigint'):
                    assert long(cql_val) == long(cqlsh_val), err_msg
                    assert long(cql_val) == long(expected_val), err_msg
                elif typ in ('float', 'double', 'decimal'):
                    assert (float(cql_val) == 0 and float(cqlsh_val) == 0) or\
                            abs((float(cql_val) - float(cqlsh_val)) / float(cql_val)) < .01, err_msg
                    assert (float(cql_val) == 0 and float(expected_val) == 0) or\
                            abs((float(cql_val) - float(expected_val)) / float(cql_val)) < .01, err_msg
                elif typ == 'blob':
                    cqlsh_val = cqlsh_val.decode('hex')
                    expected_val = expected_val.decode('hex')
                    err_msg = "A value from CQL and CQLSH did not match. CQL: '%s' CQLSH: '%s' expected: '%s'" % (str(cql_val), str(cqlsh_val), str(expected_val))
                    assert cql_val == cqlsh_val, msg
                    assert cql_val == expected_val, msg
            
                col_index += 1
            row_index += 1
                
            




