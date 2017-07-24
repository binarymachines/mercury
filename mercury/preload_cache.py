#!/usr/bin/env python

'''Usage: preload_cache.py <initfile> (--bucket <s3_bucket>)
'''

from __future__ import division
import snap
import sqldbx
import getpass
import datetime, time
import os
import json
import logging
import os
import docopt
import yaml
import arrow
import tablib
import boto3
from snap import common
import couchbase
import constants as const
import tdxutils as tdx
from tdxutils import jsonpretty
import rs_export_queries
import keygen
from keygen import *
import tempfile



export_query_template_tbl = {
    'dates': rs_export_queries.dates_xquery,
    'brands': rs_export_queries.brands_xquery,
    'users': rs_export_queries.users_xquery,
    'platforms': rs_export_queries.platforms_xquery
}



def export_redshift_records_to_csv(category_name,
                                   redshift_export_context,
                                   db_connection):
    
    export_query = export_query_template_tbl[category_name]
        
    export_file_prefix = '/'.join(['tdx', 'cache_preload_export', category_name])

    print 'Exporting %s from Redshift to CSV...' % category_name


    redshift_export_context.export_records(db_connection,                                            
                                           export_query, 
                                           export_file_prefix,
                                           options_string="delimiter '|' escape allowoverwrite")
        
    print 'Export complete.'
    print 'Downloading CSV file(s) from S3...'
    temp_download_dir = os.path.join(os.getcwd(), 'temp')

    # download from S3
    s3 = boto3.client('s3')    
    csv_filenames = []

    temp_csv_filetuples = []
    
    for key in s3.list_objects(Bucket=redshift_export_context.s3_bucket_name, Prefix=export_file_prefix)['Contents']:
        obj_name = key['Key']

        print 'bucket object: %s' % obj_name
        print 's3 prefix: %s' % export_file_prefix
        
        fileobj = tempfile.mkstemp(obj_name.split('/')[1])
        temp_csv_filetuples.append(fileobj)

        print 'Downloading %s...' % fileobj[1]
        with open(fileobj[1], "wb") as f:
            s3.download_fileobj(redshift_export_context.s3_bucket_name, obj_name, f)

    return { category_name: [t[1] for t in temp_csv_filetuples] }
             


def preload_date_records(filename, couchbase_cache, delimiter='|'):
    num_records_preloaded = 0
    with open(filename, 'r') as f:
        while True:
            record = f.readline()
            if not record:
                break
            fields = record.split(delimiter)
            date_key = fields[0]
            date_value = arrow.get(fields[1])

            cache_key = keygen.generate_date_cache_key(date_value)
            try:
                couchbase_cache.bucket.insert(cache_key, {'id': date_key })
                num_records_preloaded += 1
                if not num_records_preloaded % 1000:
                    print '%d date records preloaded.' % num_records_preloaded
            except couchbase.exceptions.KeyExistsError, err:
                pass

    print '%d date records preloaded.' % num_records_preloaded


    
def preload_user_records(filename, couchbase_cache, delimiter='|'):
    num_records_preloaded = 0
    with open(filename, 'r') as f:
        while True:
            record = f.readline()
            if not record:
                break
            fields = record.split(delimiter)
            user_key = int(fields[0])

            cache_key = keygen.generate_user_cache_key(user_key)
            try:
                couchbase_cache.bucket.insert(cache_key, {'id': user_key })
                num_records_preloaded += 1
                if not num_records_preloaded % 100000:
                    print '%d user records preloaded.' % num_records_preloaded
            except couchbase.exceptions.KeyExistsError, err:
                pass

    print '%d user records preloaded.' % num_records_preloaded
    

    
def preload_brand_records(filename, couchbase_cache, delimiter='|'):
    num_records_preloaded = 0
    with open(filename, 'r') as f:
        while True:
            record = f.readline()
            if not record:
                break
            fields = record.split(delimiter)
            brand_key = int(fields[0])

            cache_key = keygen.generate_brand_cache_key(brand_key)
            try:
                couchbase_cache.bucket.insert(cache_key, {'id': brand_key })
                num_records_preloaded += 1
                if not num_records_preloaded % 100000:
                    print '%d brand records preloaded.' % num_records_preloaded
            except caouchbase.exceptions.KeyExistsError, err:
                pass
            
    print '%d brand records preloaded.' % num_records_preloaded



def preload_platform_records(filename, couchbase_cache, delimiter='|'):
    num_records_preloaded = 0
    with open(filename, 'r') as f:
        while True:
            record = f.readline()
            if not record:
                break
            fields = record.split(delimiter)
            platform_key = int(fields[0])

            cache_key = keygen.generate_platform_cache_key(platform_key)
            try:
                couchbase_cache.bucket.insert(cache_key, {'id': platform_key })
                num_records_preloaded += 1
                if not num_records_preloaded % 100000:
                    print '%d platform records preloaded.' % num_records_preloaded
            except couchbase.exceptions.KeyExistsError, err:
                pass
            
    print '%d platform records preloaded.' % num_records_preloaded


    
def main(args):
    
    init_filename = args['<initfile>']
    yaml_config = common.read_config_file(init_filename)
    log_filename = yaml_config['globals'].get('logfile', 'cache_preload.log')
    log = tdx.init_logging('METL_cache_preload', log_filename, logging.DEBUG)
    service_objects = snap.initialize_services(yaml_config, log)
    so_registry = common.ServiceObjectRegistry(service_objects)

    couchbase = so_registry.lookup('couchbase')
    couchbase_cache = couchbase.cache_manager

    s3_bucket_name = args['<s3_bucket>']

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    rs3c = tdx.RedshiftS3Context(s3_bucket_name, aws_access_key_id, aws_secret_key)
    
    redshift_svc = so_registry.lookup('redshift_sandbox')
    password = os.getenv('REDSHIFT_PASSWORD')
    redshift_svc.login(password)
    
    start_time = time.time()

    with redshift_svc.get_connection() as conn:
        
        filemap = export_redshift_records_to_csv('dates', rs3c, conn)
        for filename in filemap['dates']:
            preload_date_records(filename, couchbase_cache)
            
        
        filemap.update(export_redshift_records_to_csv('users', rs3c, conn))
        for filename in filemap['users']:
            preload_user_records(filename, couchbase_cache)


        filemap.update(export_redshift_records_to_csv('brands', rs3c, conn))
        for filename in filemap['brands']:
            preload_brand_records(filename, couchbase_cache)


        filemap.update(export_redshift_records_to_csv('platforms', rs3c, conn))
        for filename in filemap['platforms']:
            preload_platform_records(filename, couchbase_cache)


    print '### export filemap:'
    print tdx.jsonpretty(json.dumps(filemap))
            
    end_time = time.time()
    print 'Total running time %s' % (tdx.hms_string(end_time - start_time))

    
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
