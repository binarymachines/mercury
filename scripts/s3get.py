#!/usr/bin/env python

'''Usage: s3dl.py --b=<bucket_name> --k=<s3_object_key> 

'''

import docopt
import logging
import prx_services as prx



def main(args):
    s3key = args['--k']
    bucket_name = args['--b']
    log = logging.getLogger('test')
    s3so = prx.S3ServiceObject(log, bucket=bucket_name)
    result = s3so.download_object(s3key)

    print('%s downloaded from S3.' % result)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
