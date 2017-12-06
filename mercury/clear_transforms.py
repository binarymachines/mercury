#!/usr/bin/env python

'''Usage: clear_transforms.py <init_file>
'''

import docopt
from snap import common
import redisx
import tdx_services
import tdxutils as tdx
import logging

LOG_TAG = 'METL_clear_transforms'


def main(args):
    init_filename = args['<init_file>']
    yaml_config = common.read_config_file(init_filename)
    logger = tdx.init_logging(LOG_TAG, 'reset.log', logging.DEBUG)
    service_objects = snap.initialize_services(yaml_config, logger)
    so_registry = common.ServiceObjectRegistry(service_objects)
    redis_svc = so_registry.lookup('redis')
    couchbase_svc = so_registry.lookup('couchbase')

    transforms = redis_svc.transformed_record_queue
    num_records_cleared = 0
    while True:
        key = transforms.pop()
        if not key:
            break
        try:            
            couchbase_svc.data_manager.bucket.remove(key)
            num_records_cleared += 1
            if not num_records_cleared % 100000:
                print '%d records cleared.' % num_records_cleared
        except Exception, err:
            print('%s thrown while clearing record ID %s: %s' % (err.__class__.__name, key, err.message))

    print('%d records cleared.' % num_records_cleared)
    print('exiting.')
        

    
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
