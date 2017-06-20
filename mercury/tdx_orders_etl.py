#!/usr/bin/env python


'''Usage: tdx_orders_etl.py -i <initfile_name> -b <target_bucket> [--skip-cache-preload]
'''

import os, sys
import docopt
import subprocess
import traceback

import preload_cache as preload
import extract_order_lineitems as extract
import transform_order_lineitems as transform
import load_order_lineitems as load



def main(args):
    initfile = args['<initfile_name>']
    s3_bucket_name = args['<target_bucket>']
    skip_preload = args['--skip-cache-preload']

    extract_framesize = 50000
    extract_num_frames = 1
    transform_framesize = 100000
    transform_num_frames = 1
    load_framesize = 100000
    load_num_frames = 1

    preload_params = { '<initfile>' : initfile,
                       '<s3_bucket>' : s3_bucket_name }

    extract_params = { '<initfile>' : initfile,
                       '<num_frames>': int(extract_num_frames),
                       '<framesize>' : int(extract_framesize),
                       '<s3_bucket>' : s3_bucket_name }

    transform_params = {'<initfile>' : initfile,
                        '<num_frames>' : int(transform_num_frames),
                        '<framesize>' : int(transform_framesize) }

    load_params = {'<initfile>'  : initfile,
                   '<num_frames>' : int(load_num_frames),
                   '<framesize>' : int(load_framesize),
                   '<s3_bucket>' : s3_bucket_name }
    
    try:    
        if not skip_preload:
            preload.main(preload_params)
            
        #extract.main(extract_params)
        #transform.main(transform_params)
        load.main(load_params)
        
    except Exception, err:
        print 'ETL run failed with %s: %s' % (err.__class__.__name__, err.message)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stderr)

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
    
