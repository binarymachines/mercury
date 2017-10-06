#!/usr/bin/env python

'''Usage:     eavesdrop <initfile> <channel>
'''

import os, sys
import docopt
import yaml
import pgpubsub
import logging
from snap import snap, common


class NoSuchEventChannel(Exception):
    def __init__(self, channel_id):
        Exception.__init__(self,
                           'No event channel registered under the name "%s". Please check your initfile.' % channel_id)

        
class NoSuchEventHandler(Exception):
    def __init__(self, handler_func_name, handler_module):
        Exception.__init__(self, 'No event handler function "%s" exists in handler module "%s". Please check your initfile and code modules.' % (handler_func_name, handler_module))


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

        
        
def main(args):
    print args

    local_env = common.LocalEnvironment('PGSQL_USER', 'PGSQL_PASSWORD')
    local_env.init()

    pgsql_user = local_env.get_variable('PGSQL_USER')
    pgsql_password = local_env.get_variable('PGSQL_PASSWORD')

    yaml_config = common.read_config_file(args['<initfile>'])

    db_host = yaml_config['globals']['database_host']
    db_name = yaml_config['globals']['database_name']
    
    pubsub = pgpubsub.connect(host=db_host,
                              user=pgsql_user,
                              password=pgsql_password,
                              database=db_name)

    channel_id = args['<channel>']

    if not yaml_config['channels'].get(channel_id):
        raise NoSuchEventChannel(channel_id)

    handler_module_name = yaml_config['globals']['handler_module']

    project_dir = common.load_config_var(yaml_config['globals']['project_dir'])
    sys.path.append(project_dir)
    handlers = __import__(handler_module_name)
    handler_function_name = yaml_config['channels'][channel_id]['handler_function']
    
    if not hasattr(handlers, handler_function_name):
        raise NoSuchEventHandler(handler_function_name, handler_module_name)

    handler_function = getattr(handlers, handler_function_name)
    service_objects = common.ServiceObjectRegistry(snap.initialize_services(yaml_config, logger))
    
    pubsub.listen(channel_id)
    print 'listening on channel "%s"...' % channel_id
    for event in pubsub.events():
        print event.payload
    


        
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
