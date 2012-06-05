import pprint
import random
import urllib2 # to quote strings
import os
import simplejson

try:
    import ipdb
except ImportError, e:
    pass

from keyspace_configuration import KeyspaceConfiguration
from column_family_configuration import ColumnFamilyConfiguration, \
        StandardColumnFamilyConfiguration, CounterColumnFamilyConfiguration
from debug import debug

"""
This file is the main entry point to the CQLSH/CQL tests.

it outputs to 3 log files:
 - last_succeeded_cql.log
 - last_failed_cql.log
 - cqlsh_output.log
The first two files have to do with the most recent test.
The last logs all output (and input) sent through pexpect to and from cqlsh

environment variables that can be set:
CQL_VERSION: set the version of cql to use

CQL_CQLSH_TEST: A string such as the following is output in the log files, 
and in the error message for failed tests. Exporting the given string and running 
the tests again will ensure that the specified test will be run first.
'to recreate this test, run this command before re-running the test: export CQL_CQLSH_TEST="%7B%22classname%22%3A%20%22StandardColumnFamilyConfiguration%22%2C%20%22keyspace_configuration%22%3A%20%7B%22classname%22%3A%20%22KeyspaceConfiguration%22%2C%20%22config_params%22%3A%20%7B%22strategy_options%3Areplication_factor%22%3A%20%221%22%2C%20%22strategy_class%22%3A%20%22SimpleStrategy%22%7D%7D%2C%20%22config_params%22%3A%20%7B%22key_validation_class%22%3A%20%22text%22%2C%20%22comment%22%3A%20%22%27a%20comment%27%22%2C%20%22min_compaction_threshold%22%3A%204%2C%20%22default_validation%22%3A%20%22varchar%22%2C%20%22comparator%22%3A%20%22varchar%22%2C%20%22gc_grace_seconds%22%3A%20864000%2C%20%22max_compaction_threshold%22%3A%2016%2C%20%22replicate_on_write%22%3A%20%22false%22%2C%20%22disposition%22%3A%20%22dynamic%22%2C%20%22read_repair_chance%22%3A%201.0%7D%7D"'

"""


def get_all_configuration_permutations(connection):
    """
    returns a list of all possible column families and their keyspaces.
    """
    configurations = []
    keyspace_param_permutations = KeyspaceConfiguration.get_parameter_permutations()
    for keyspace_param_permutation in keyspace_param_permutations:
        permutation_dict = dict(keyspace_param_permutation)
        keyspace = KeyspaceConfiguration(connection, **permutation_dict)


        # loop over all the types of column families
        for cls in (StandardColumnFamilyConfiguration,):# CounterColumnFamilyConfiguration):
            # standard column families
            scf_param_permutations = cls.get_parameter_permutations()
            for scf_param_permutation in scf_param_permutations:
                scf_perm_dict = dict(scf_param_permutation)
                if cls.is_configuration_valid(scf_perm_dict):
                    cf = cls(connection, keyspace, **scf_perm_dict)
                    configurations.append(cf)
        
    return configurations


def get_failed_configuration(connection):
    """
    looks for an environment variable named CQL_CQLSH_TEST, and creates
    a configuration for that test.
    """
    env_str = os.environ.get('CQL_CQLSH_TEST', None)
    if not env_str:
        return None

    unquoted_str = urllib2.unquote(env_str)
    obj = simplejson.loads(unquoted_str)

    debug("Got a test to re-run", obj)

    # recreate the keyspace configuration
    ks_obj = obj['keyspace_configuration']
    keyspace_configuration = globals()[ks_obj['classname']](connection, **ks_obj['config_params'])

    # recreate the columnfamily configuration
    cls = globals()[obj['classname']]
    config = obj['config_params']
    # make sure it is valid first
    if not cls.is_configuration_valid(config):
        debug('The failed test is not valid. To clear it out run this command: export CQL_CQLSH_TEST=')
        return None
    try:
        cf_config = cls(connection, keyspace_configuration, **config)
    except Exception, e:
        e.args = e.args + ('Could not create the failed test. To clear it out run this command: export CQL_CQLSH_TEST=',)
        raise
    return cf_config


def iterate_over_configurations(conn, max_num=40):
    confs = get_all_configuration_permutations(conn)
    random.shuffle(confs)

    # put the failed test at the front, if there is one.
    failed = get_failed_configuration(conn)
    if failed:
        confs = [failed,] + confs

    count = 0
    for conf in confs:
        config_str = urllib2.quote(conf.get_json_string())
        repeat_string = 'to recreate this test, run this command before re-running the test: '\
                'export CQL_CQLSH_TEST="%s"' % config_str
        debug(repeat_string)
        try:
            conf.run_everything()
            # log all the CQL to a file for easy reproduction
            fle = open('last_succeeded_cql.log', 'w')
            fle.write(repeat_string + "\n\n")
            fle.write(conf.get_log_string())
            fle.close()
            conf.cleanup()
        except Exception, e:
            fle = open('last_failed_cql.log', 'w')
            fle.write(repeat_string + "\n\n")
            fle.write(conf.get_log_string())
            fle.close()

            e.args = e.args + (repeat_string,)
            import sys
            tb = sys.exc_info()[2]

            # to drop into debug mode when something fails, uncomment this line.
            # (easy_install ipdb first)
#            ipdb.post_mortem(tb)
            raise
        count += 1
        if count >= max_num:
            debug("Max count reached. Leaving.")
            break


if __name__ == '__main__':
    "Run the test now."
    import sys
    from multi_connection import MultiConnection
    try:
        host, port, cassandra_dir = sys.argv[1:]
    except ValueError, e:
        print """
Run the cqlsh tests like this:
python configuration_factory.py <host> <port> <cassandra_dir>
        """
        sys.exit(1)
    conn = MultiConnection(host, port, cassandra_dir)
    iterate_over_configurations(conn)

