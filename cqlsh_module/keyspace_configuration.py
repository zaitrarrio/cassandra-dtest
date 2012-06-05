from configuration import Configuration

class KeyspaceConfiguration(Configuration):
    """
    manages configuration for a keyspace
    """

    @classmethod
    def _get_available_parameters(cls):
        "Returns a dict of all available configuration parameters"
        out = {
            'strategy_class': ['SimpleStrategy',],
            'strategy_options:replication_factor': ['1', '3'],
        }
        s_out = super(KeyspaceConfiguration, cls)._get_available_parameters()
        out = dict( out.items() + s_out.items() )
        return out    

    def setup(self):
        options_str = ' AND '.join(name + '=' + value for name, value in self.config_params.items())
        if self.db_name:
            self.create_ks(self.db_name, options_str)
        else:
            self.create_ks('ks_'+self.get_hash(), options_str)
        
