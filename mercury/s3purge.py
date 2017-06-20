#!/usr/bin/env python

'''Usage: s3purge.py <bucket_name>
'''


import docopt
import boto3

def main(args):

    bucket_name = args['<bucket_name>']

    print 'purging S3 bucket "%s"...' % bucket_name
    #s3 = boto3.resource('s3') 
    target_bucket = boto3.resource('s3').Bucket(bucket_name)
    for key in target_bucket.objects.all():
        key.delete()
    
    print 'Done.\n'

    
if __name__=='__main__':
    args = docopt.docopt(__doc__)
    main(args)

