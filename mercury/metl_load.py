#!/usr/bin/env python

'''Usage: load_order_lineitems.py <initfile> [--segment=<segment_number>] (--frames <num_frames>) (--size <framesize>) (--bucket <s3_bucket>) [-p ]


   Options: 
       -p  --preview    generate (but do not upload) output records 
       -s  --segment    only process records with this work segment number
       -f  --frames     number of consecutive reading frames to open
       -b  --bucket     target S3 bucket
'''

from __future__ import division
import snap
import couchbasedbx as cbx
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
from couchbase.n1ql import N1QLQuery
from couchbase.exceptions import *
from snap import common
import constants as const
import cb_queries
import tdxutils as tdx
from tdxutils import jsonpretty
from tdxutils import ReadingFrame

LOG_TAG = 'METL_load'


class DocumentSegmentReader(object):
    def __init__(self):
        pass


    def get_docset(self, segment, reading_frame, record_id_queue, couchbase_pm):
        log = logging.getLogger('METL_load')
        log.info('retrieving keys from redis...')
        range_start = reading_frame.index_number * reading_frame.size
        range_end = range_start + reading_frame.size - 1
        ids = record_id_queue.range(range_start, range_end)        
        results = []        
        for id in ids:
            raw_record = couchbase_pm.lookup_record_raw(id)
            if not raw_record:
                raise Exception('queued record ID %s has no matching record in couchbase.' % id)
            couchbase_record = cbx.CouchbaseRecordBuilder(const.RECTYPE_ORDER_TRANSFORMED).add_fields(raw_record).build()
            results.append(couchbase_record.source_record)

        log.info('%d keys retrieved.' % len(results))
        return results


def generate_date_cache_key(date_value):
    return 'date_%s' % date_value.isoformat()


    
def get_date_dim_tbl_pk_for_record(lineitem_rec, db_conn, couchbase_cache):
    rec_datestamp = lineitem_rec['order_placement_timestamp']
    target_date = arrow.get(rec_datestamp).date()
    cache_key = generate_date_cache_key(target_date)
    id = None
    
    try:
        date_doc = couchbase_cache.bucket.get(cache_key)        
        id = date_doc.value['id']
    except NotFoundError:
        query = "select key from d_dates where date_value = '%s'" % target_date

        results = []
        result_set = db_conn.execute(query)
        for row in result_set:
            results.append(tdx.db_record_to_dictionary(row))

        if len(results) > 1:
            raise Exception('The query: \n%s\n should return no more than one record.' % query)

        if not len(results):
            raise Exception('The query: \n%s\n should return no less than one record.' % query)

        id = results[0]['key']

        couchbase_cache.bucket.insert(cache_key, {'id': id })
                
    return id


def generate_brand_cache_key(brand_id):
    return 'brand_%d' % brand_id


def get_brand_dim_tbl_pk_for_record(lineitem_rec, db_conn, couchbase_cache):
    brand_id = lineitem_rec['brand_id']
    cache_key = generate_brand_cache_key(brand_id)
    id = None

    try:
        brand_doc = couchbase_cache.bucket.get(cache_key)
        id = brand_doc.value['id']
    except NotFoundError:
        query = "select key from d_brands where key = %d" % brand_id
        results = []
        result_set = db_conn.execute(query)
        for row in result_set:
            results.append(tdx.db_record_to_dictionary(row))

        if len(results) > 1:
            raise Exception('The query: \n%s\n should return no more than one record.' % query)
        if len(results):                    
            id = results[0]['key']
            couchbase_cache.bucket.insert(cache_key, {'id': id})
        
    return id



def generate_user_cache_key(user_id):
    return 'user_%d' % user_id



def get_user_dim_tbl_pk_for_record(lineitem_rec, db_conn, couchbase_cache):

    user_id = lineitem_rec['shopper_id']
    cache_key = generate_user_cache_key(user_id)
    id = None

    try:
        user_doc = couchbase_cache.bucket.get(cache_key)
        id = user_doc.value['id']
        
    except NotFoundError:
        query = "select key from d_users where key=%d" % user_id
        results = []
        result_set = db_conn.execute(query)
        for row in result_set:
            results.append(tdx.db_record_to_dictionary(row))

        if len(results) > 1:
            raise Exception('The query: \n%s\n should return no more than one record.' % query)

        if len(results):
            id = results[0]['key']
            couchbase_cache.bucket.insert(cache_key, {'id': id})
            
    return id



def generate_platform_cache_key(platform_id):
    return 'platform_%s' % platform_id
    


def get_platform_dim_tbl_pk_for_record(lineitem_rec, db_conn, couchbase_cache):

    platform_id = lineitem_rec['shopper_platform']
    cache_key = generate_platform_cache_key(platform_id)
    id = None

    try:        
        platform_doc = couchbase_cache.bucket.get(cache_key)
        id = platform_doc.value['id']

    except NotFoundError:
        query = "select key from d_shopper_platforms where name='%s'" % platform_id
        results = []
        result_set = db_conn.execute(query)
        for row in result_set:
            results.append(tdx.db_record_to_dictionary(row))

        if len(results) > 1:
            raise Exception('The query: \n%s\n should return no more than one record.' % query)  

        if len(results):
            id = results[0]['key']
            couchbase_cache.bucket.insert(cache_key, {'id': id})
            
    return id


    
def convert_key_spec_array_to_key_dictionary(key_spec_array):
    key_dictionary = {}
    for key_spec in key_spec_array:
        key_dictionary[key_spec['key_type']] = key_spec['key_value']

    return key_dictionary

    
def compile_fact_records(src_doc, db_conn, cb_cache):

    rec_datestamp = src_doc['order_placement_timestamp']
    target_date = arrow.get(rec_datestamp).date()
    target_brand = src_doc['brand_id']
    target_user = src_doc['shopper_id']
    target_shopper_platform = src_doc['shopper_platform']

    date_pk = get_date_dim_tbl_pk_for_record(src_doc, db_conn, cb_cache)
    brand_pk = get_brand_dim_tbl_pk_for_record(src_doc,  db_conn, cb_cache)
    user_pk = get_user_dim_tbl_pk_for_record(src_doc, db_conn, cb_cache)
    shopper_platform_pk = get_platform_dim_tbl_pk_for_record(src_doc, db_conn, cb_cache)

    outbound_records = []
    for item in src_doc['merchandise_lineitems']:
        d = {}
        d['ol_key'] = None
        d['ol_date_key'] = date_pk
        d['ol_brand_key'] = brand_pk
        d['ol_user_key'] =  user_pk
        d['ol_platform_key'] =  shopper_platform_pk
        d['ol_order_number'] = item['order_id']
        d['ol_product_number'] = item['product_id']
        d['ol_sku_number'] = item['sku_key']
        d['ol_quantity'] = item['sku_quantity']

        outbound_records.append(d)

    return outbound_records
    


def write_to_star_schema(fact_record, fact_table):    
    i = fact_table.insert()    
    i.execute(fact_record)

    

def main(args):
    print(args)
    init_filename = args['<initfile>']
    segment_num = -1
    if args.get('--segment') is not None:
        segment_num = int(args['--segment'])
    num_reading_frames = int(args.get('<num_frames>', -1))
    reading_frame_size = int(args.get('<framesize>', -1))
    preview_mode = False
    if args.get('--preview') or args.get('-p'):
        preview_mode = True
        log.info('running load script in preview mode.')
    
    yaml_config = common.read_config_file(init_filename)
    pipeline_id = yaml_config['globals']['pipeline_id']

    log_directory = yaml_config['globals']['log_directory']
    job_id = tdx.generate_job_id('load', pipeline_id)
    log_filename = tdx.generate_logfile_name(job_id)
    log = tdx.init_logging('METL_load', os.path.join(log_directory, log_filename), logging.DEBUG)

    print('%s script started at %s, logging to %s...' % (LOG_TAG, datetime.datetime.now().isoformat(), log_filename))
    
    service_objects = snap.initialize_services(yaml_config, log)
    so_registry = common.ServiceObjectRegistry(service_objects)

    redis_svc = so_registry.lookup('redis')
    cooked_record_id_queue = redis_svc.get_transformed_record_queue(pipeline_id)
    
    couchbase = so_registry.lookup('couchbase')
    cb_cache = couchbase.cache_manager

    redshift_svc = so_registry.lookup('redshift_sandbox')
    password = os.getenv('REDSHIFT_PASSWORD')
    redshift_svc.login(password)
    s3_bucket_name = args['<s3_bucket>']
    
    start_time = time.time()

    paging_context = tdx.PagingContext(num_reading_frames, reading_frame_size)        
    pages_remaining = paging_context.num_frames_to_read
    reading_frame = paging_context.initial_reading_frame()
    dsr = DocumentSegmentReader()
    record_headers = ['ol_date_key', 'ol_brand_key', 'ol_user_key', 'ol_platform_key', 'ol_order_number', 'ol_product_number', 'ol_sku_number', 'ol_quantity']
    output_data = tablib.Dataset()
    num_facts_compiled = 0
    num_orders_processed = 0
    
    while pages_remaining:
        with redshift_svc.get_connection() as conn:
            log.info('retrieving order keys for page %d...' % reading_frame.index_number)
            docs = dsr.get_docset(segment_num, reading_frame, cooked_record_id_queue, couchbase.data_manager)            
            log.info('compiling fact records for page %d...' % reading_frame.index_number)
            for doc in docs:
                frecords = compile_fact_records(doc, conn, cb_cache)            
                for fact in frecords:
                    num_facts_compiled += 1
                    output_fields = [fact[key] for key in record_headers]
                    output_data.append(output_fields)
                    if not num_facts_compiled % 50000:
                        log.info('%d fact records compiled.' % num_facts_compiled)

            num_orders_processed += len(docs)
            reading_frame = reading_frame.shift_right()
            pages_remaining -= 1

    log.info('%d fact records compiled.' % num_facts_compiled)
    output_filename = 'olap_star_schema_records_segment_%d.csv' % segment_num
    with open(output_filename, 'wb') as output_file:
        output_file.write(output_data.csv)

    if preview_mode:
        exit()
        
    s3 = boto3.client('s3')
    s3_object_key = 'tdx/%s' % output_filename        
    log.info('uploading datafile %s to S3 bucket %s using key %s...' % (output_filename, s3_bucket_name, s3_object_key))
    s3.upload_file(output_filename, s3_bucket_name, s3_object_key)

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    rs3c = tdx.RedshiftS3Context(s3_bucket_name, aws_access_key_id, aws_secret_key)
    
    password = os.getenv('REDSHIFT_PASSWORD')
    redshift_svc.login(password)

    with redshift_svc.get_connection() as conn:
        log.info('copying data into Redshift...')
        copy_cmd = rs3c.import_records(conn, 'f_order_lineitems', record_headers, s3_object_key)
         
    end_time = time.time()
    log.info('%d orders processed into %d fact records.' % (num_orders_processed, num_facts_compiled))
    log.info('Total running time %s' % (tdx.hms_string(end_time - start_time)))

    print('Exiting.')
    
    
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)


    
