import copy
from configuration import Configuration
import data_generation
import debug

#validation_classes = [
#    'AsciiType', 'BytesType', 'Int32Type', 'IntegerType',
#    'LexicalUUIDType', 'LongType', 'UTF8Type',
#]

datatypes = [
    'ascii', 'blob', 'text', 'varchar',
    #'decimal', 'double', 'float', 'varint', 'int', 'bigint', 'uuid', 'boolean', 
    # The above types cause an error preventing testing, due to this bug:
    # https://issues.apache.org/jira/browse/CASSANDRA-4002
]

# You have to have a cluster with at least 2 nodes for consistency-levles
# greater then ONE.
consistency_levels = ['ANY', 'ONE',]

class ColumnFamilyConfiguration(Configuration):
    """
    manages configuration for a column family
    """

    @classmethod
    def _get_available_parameters(cls):
        "Returns a dict of all available configuration parameters."
        out = {
            # TODO: Figure out why this isn't working. Is my version of CQL not good enough?
            # tpatterson p.    Anyone know how to create a columnfamily in cql without using SnappyCompressor?
            # Sylvain L:       tpatterson using sstable_compression: "" should work.
#            'compression_options': 
#                ['{sstable_compression:}', 
#                '{sstable_compression:SnappyCompressor}', 
#                '{sstable_compression:DeflateCompressor}'],
            'disposition': ['static', 'dynamic',], # static means specific columns, dynamic is more for time-series data.
            'comment': ["'a comment'", "''"],
            'read_repair_chance': [0.2, 0.0, 1.0],
            'gc_grace_seconds': [1000, 864000,],
            'min_compaction_threshold': [2, 4],
            'max_compaction_threshold': [16, 32],
            'replicate_on_write': ['false', 'true'],
        }
        s_out = super(ColumnFamilyConfiguration, cls)._get_available_parameters()
        out = dict( out.items() + s_out.items() )
        return out


    @classmethod
    def is_configuration_valid(cls, config):
        """
        call this function to see if the configuration
        is valid. 
        """
        return True

    def __init__(self, connection, keyspace_configuration, **kwargs):
        self.keyspace_configuration = keyspace_configuration
        super(ColumnFamilyConfiguration, self).__init__(connection, **kwargs)

    def get_cf_name(self):
        if self.db_name:
            return self.db_name
        else:
            return ('cf_' + self.get_hash())[:32]

    def _get_json_obj(self):
        out = super(ColumnFamilyConfiguration, self)._get_json_obj()
        out['keyspace_configuration'] = self.keyspace_configuration._get_json_obj()
        return out

    def setup(self):
        """
        Creates the column family and keyspace (if needed)
        """
        self.keyspace_configuration.setup()

        skip_keys = ['key_validation_class', 'disposition',]
        config_str = ' AND '.join(str(key) + '=' + str(value) for key, value in self.config_params.items() if key not in skip_keys)

        extra_cols = self.get_static_columns()
        if extra_cols:
            extra_columns_str = ', ' + ', '.join('%s %s'%(c_name, c_type) for c_name, c_type in extra_cols)
        else:
            extra_columns_str = ''

        cql_str = "CREATE COLUMNFAMILY %s (KEY %s PRIMARY KEY%s) WITH %s;" % (
            self.get_cf_name(),
            self.config_params['key_validation_class'],
            extra_columns_str,
            config_str
            )

        self.create_cf(cql_str, self.get_cf_name())


    def run_everything(self):
        """
        A helper function to run setip, inserts, updates, etc.
        """
        self.setup()
        self.insert()
        self.validate()

    
    def get_log_string(self):
        out = self.keyspace_configuration.get_log_string()
        out += "\n" + super(ColumnFamilyConfiguration, self).get_log_string()
        return out


class StandardColumnFamilyConfiguration(ColumnFamilyConfiguration):
    """
    manages configuration for a column family
    """
    def __init__(self, *argv, **kwargs):
        super(StandardColumnFamilyConfiguration, self).__init__(*argv, **kwargs)

    @classmethod
    def _get_available_parameters(cls):
        "Returns a dict of all available configuration parameters"
        out = {
            'comparator': datatypes,
            'default_validation': datatypes,
            'key_validation_class': ['text'],# datatypes,
        }
        s_out = super(StandardColumnFamilyConfiguration, cls)._get_available_parameters()
        out = dict( out.items() + s_out.items() )
        return out

    @classmethod
    def is_configuration_valid(cls, config):
        """
        call this function to see if the configuration
        is valid. 

        'ascii', 'bigint', 'blob', 'boolean', 'decimal', 'double', 'float', 'int',
        'text', 'uuid', 'varchar', 'varint',
        """

        # all the different comparators only apply to dynamic column families.
        if config['disposition'] == 'static' and config['comparator'] != 'varchar':
            return False

        validation = config['default_validation']
        comparator = config['comparator']

        # for these types, make sure the comparator matches. TODO: Look at relaxing some
        # of these requirements.
        if validation in ('bigint', 'boolean', 'decimal', 'double', 'blob', 'ascii',
                'float', 'int', 'uuid', 'varint') and comparator != validation:
            return False

        if validation in ('text', 'varchar') and \
                comparator not in ('text', 'varchar'):
            return False

        return super(StandardColumnFamilyConfiguration, cls).is_configuration_valid(config)


    def get_static_columns(self):
        """
        returns a list of 2-tuples like this:
        (<column_name>, <column_type>)
        each datatype is listed multiple times so that we can delete
        some and still have some left to check.
        """
        out = []
        if self.config_params['disposition'] == 'static':
            for i in xrange(4):
                for dt in datatypes:
                    cf_name = 'col_%d_%s' % (i, dt)
                    out.append((cf_name, dt))

        return out

    
    def insert(self):
        """
        inserts some data.
        """
        rows = data_generation.generate_cf_data(
                self.config_params['key_validation_class'],
                self.config_params['default_validation'], 
                self.get_static_columns())
       
        row_num = 0
        for row in rows:
            col_names = []
            col_values = []
            for col in row['columns']:
                col_names.append(col['name'])
                col_values.append(col['value'])
            consistency_level = consistency_levels[row_num % len(consistency_levels)]    
            cql_str = "INSERT INTO %s (KEY, %s) VALUES ('%s', %s) USING CONSISTENCY %s;" %(
                self.get_cf_name(),
                ', '.join("'" + str(a) + "'"for a in col_names),
                row['key'],
                ', '.join("'" + str(a) + "'" for a in col_values),
                consistency_level,
            )
            row_num += 1
            self.execute(cql_str)

    def validate(self):
        rows = data_generation.generate_cf_data(
                self.config_params['key_validation_class'],
                self.config_params['default_validation'], 
                self.get_static_columns())

        keys = ("'" + str(row['key']) + "'" for row in rows)
        cql_str = "SELECT * FROM %s WHERE KEY IN (%s);" % (
                self.get_cf_name(), ', '.join(keys))

        if self.config_params['disposition'] == 'dynamic':
            self.select_dynamic(cql_str, rows)
        else:
            self.select_static(cql_str, rows)


class CounterColumnFamilyConfiguration(ColumnFamilyConfiguration):
    """
    manages configuration for a column family
    """

    @classmethod
    def _get_available_parameters(cls):
        "Returns a dict of all available configuration parameters"
        out = {
#            'comparator': ['CounterColumnType'],
            'default_validation': ['CounterColumnType',],
            'key_validation_class': datatypes,
        }
        s_out = super(CounterColumnFamilyConfiguration, cls)._get_available_parameters()
        out = dict( out.items() + s_out.items() )
        return out

#    @classmethod
#    def is_configuration_valid(cls, config):
#        """
#        call this function to see if the configuration
#        is valid. 
#        """
#        if config['disposition'] == 'static' and config['comparator'] != 'varchar':
#            return False

#        return super(CounterColumnFamilyConfiguration, cls).is_configuration_valid(config)


    def get_static_columns(self):
        """
        returns a list of 2-tuples of column name names and types.
        The default is not to return anything.
        """
        return []
