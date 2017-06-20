#!/usr/bin/env python

'''Usage: s3push.py <local_file> (-b <s3_bucket>) [<bucket_location>]

'''

import boto3
import docopt
import os


def main(args):

    local_filename = args['<local_file>']
    bucket_name = args['<s3_bucket>']
    bucket_location = args.get('<bucket_location>') 

    if bucket_location:
        bucket_path = os.path.join(bucket_location, os.path.basename(local_filename))
    else:
        bucket_path = os.path.basename(local_filename)

    s3 = boto3.resource('s3')
    print 'uploading file %s to bucket %s with key [%s]...' % (local_filename, bucket_name, bucket_path)
    with open(local_filename) as f:
        s3.meta.client.put_object(Bucket=bucket_name, Body=f, Key=bucket_path)

    print 'done.'
    




if __name__=='__main__':
    args = docopt.docopt(__doc__)
    main(args)



