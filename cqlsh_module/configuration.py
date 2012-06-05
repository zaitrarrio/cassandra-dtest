import itertools
import pprint
import hashlib
import simplejson

class Configuration(object):
    """
    manages a keyspace or column family configuration

    The subclass must define self.PARAMETERS
    """

    @classmethod
    def _get_available_parameters(cls):
        return {}

    def __init__(self, connection, **kwargs):
        self.__connection = connection
        ap = self._get_available_parameters()

        self.db_name = None
        if 'db_name' in kwargs:
            self.db_name = kwargs['db_name']
            del kwargs['db_name']

        for kkey, kvalue in kwargs.items():
            assert kkey in ap.keys(), 'invalid param passed to KeyspaceConfiguration.__init__()'
            assert kvalue in ap[kkey], 'invalid value passed to KeyspaceConfiguration.__init__()'

        self.config_params = kwargs

        # Store the cql that will recreate this test.
        self.cql_reproduction_log = []

    def __repr__(self):
        return self.__class__.__name__ + ' ' + pprint.pformat(self.config_params)

    
    def get_json_string(self):
        return simplejson.dumps(self._get_json_obj())
    

    def _get_json_obj(self):
        """
        return an object that can be json-serializable, and that will
        allow this object to be recreated.
        """
        out = {
            'classname': self.__class__.__name__,
            'config_params': self.config_params,
        }
        return out

    def get_hash(self):
        return hashlib.md5(str(self)).hexdigest()

    @classmethod
    def get_parameter_permutations(cls):
        """
        returns a list of all possible configurations.
        """
        avail_params = cls._get_available_parameters()

        # make a list of all possible options in this form:
        # param_lists = [[(param_nameA, val1), (param_nameA, val2)], [(param_nameB, val1), (param_nameB, val2)]]
        # note that the param_name is repeated for each value. 
        param_lists = []
        for param_name in avail_params.keys():
            param_entry = []
            for param_value in avail_params[param_name]:
                param_entry.append((param_name, param_value))
            param_lists.append(param_entry)
        
        all_possible_params = list(itertools.product(*param_lists))
        return all_possible_params


    def get_log_string(self):
        """
        returns a string containing all the cql needed to reproduce this test.
        """
        return '\n'.join(self.cql_reproduction_log)
    
    def cleanup(self):
        # clear out the cql_reproduction_log.
        self.cql_reproduction_log = None

    ########
    ######## An abstraction layer, so that we can track all the CQL we run.
    ########
    def create_ks(self, ks_name, options_str):
        log = self.__connection.create_ks(ks_name, options_str)
        if log:
            self.cql_reproduction_log += log

    def create_cf(self, query, cf_name):
        self.__connection.create_cf(query, cf_name)
        self.cql_reproduction_log.append(query)

    def execute(self, query):
        self.__connection.execute(query)
        self.cql_reproduction_log.append(query)

    def select_dynamic(self, query, expected_result):
        self.__connection.select_dynamic(query, expected_result)
        self.cql_reproduction_log.append(query)

    def select_static(self, query, expected_result):
        self.__connection.select_static(query, expected_result)
        self.cql_reproduction_log.append(query)



