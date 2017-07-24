#!/usr/bin/env python

'''Usage: clear_redis.py <init_file> 
'''

import docopt
from snap import common
import redisx
import tdx_services
import tdxutils as tdx
import logging

LOG_TAG = 'METL_clear_redis'


def main(args):
    init_filename = args['<init_file>']
    yaml_config = common.read_config_file(init_filename)
    pipeline_id = yaml_config['globals']['pipeline_id']
    logger = tdx.init_logging(LOG_TAG, 'reset.log', logging.DEBUG)
    service_objects = snap.initialize_services(yaml_config, logger)
    so_registry = common.ServiceObjectRegistry(service_objects)
    redis_svc = so_registry.lookup('redis')

    redis_svc.redis_server.instance().delete(redis_svc.get_transformed_record_queue(pipeline_id))
    redis_svc.redis_server.instance().delete(redis_svc.get_raw_record_queue(pipeline_id))
    redis_svc.redis_server.instance().delete(redis_svc.get_generator_to_user_map(pipeline_id))

    
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
