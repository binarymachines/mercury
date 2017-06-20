#!/usr/bin/env python

'''Usage: extract_order_lineitems.py <initfile> [--frames <num_frames>] [--size <framesize>] [--bucket <s3_bucket>]

'''


import os, fnmatch
import tempfile
from snap import couchbasedbx as cbx
from snap import sqldbx
from snap import common, snap
from snap import csvutils as csvu
from snap import journaling as journaling
from snap.journaling import *
from snap import redisx

import getpass
import time
import datetime
import json
import docopt
import rs_export_queries
import cb_queries
import yaml
import boto3
import constants as const
import metl_utils as metl
import logging

from metl import ReadingFrame, db_record_to_dictionary, jsonpretty, generate_order_key


LOG_TAG = 'METL_extract'
DEFAULT_SEGMENT_COUNT = 3



def build_exported_record_map():
    rmb = csvu.CSVRecordMapBuilder()
    rmb.add_field('id', int)

    return rmb.build(delimiter='|')



mapbuilder_func_table = {
    '<business_obj1_name>': exported_record_map_1,
    '<business_obj2_name>': exported_record_map_2
}


def export_redshift_records_to_csv(category_name,
                                   redshift_export_context,
                                   db_connection,
                                   paging_context=None,
                                   **kwargs):

    log = logging.getLogger(LOG_TAG)
    if paging_context:
        export_query = windowed_export_query_template_tbl[category_name]
    else:
        export_query = export_query_template_tbl[category_name]
        
    export_file_prefix = '/'.join([tdx.s3_export_prefix, category_name])

    log.info('Exporting %s from Redshift to CSV...' % category_name)

    if paging_context:
        pages_remaining = paging_context.num_frames_to_read
        reading_frame = paging_context.initial_reading_frame()
        
        while pages_remaining:
            page_number = (paging_context.num_frames_to_read - pages_remaining) + 1
            record_id_lower_bound = reading_frame.index_number * reading_frame.size
            record_id_upper_bound = record_id_lower_bound + reading_frame.size - 1
            redshift_export_context.export_records(db_connection,                                            
                                           export_query.format(record_id_lower_bound, record_id_upper_bound), 
                                           '%s_page_%d_' % (export_file_prefix, page_number),
                                           options_string="delimiter '|' escape allowoverwrite")
            
            reading_frame = reading_frame.shift_right()
            pages_remaining -= 1
    else:
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

        log.info('bucket object: %s' % obj_name)
        log.info('s3 prefix: %s' % export_file_prefix)
        
        fileobj = tempfile.mkstemp(obj_name.split('/')[1])
        temp_csv_filetuples.append(fileobj)

        log.info('Downloading %s...' % fileobj[1])
        with open(fileobj[1], "wb") as f:
            s3.download_fileobj(redshift_export_context.s3_bucket_name, obj_name, f)

    return { category_name: [t[1] for t in temp_csv_filetuples] }
             



def generate_obj_key(obj_type_name, record):
    return None


def import_csv_records_to_couchbase(record_type_name, csv_filenames, couchbase_svc, redis_svc, segment_counter, **kwargs):
    log = logging.getLogger(LOG_TAG)        
    mapbuilder_func = mapbuilder_func_table[record_type_name]

    log.info('Importing %s from CSV to Couchbase...' % record_type_name)
    
    num_records_imported = 0    
    csv_record_map = mapbuilder_func()
    staging_record = None

    cb_record_builder = cbx.CouchbaseRecordBuilder(record_type_name)
    
    for filename in csv_filenames:        
        with open(filename, "r") as f:
            linenum = 1
            for line in f:
                try:
                    record_data = csv_record_map.row_to_dictionary(line)
                    lineitem_record = cb_record_builder.add_fields({'source_record': record_data}).build()
                
                    # staging record will be stored in Redis and will point to a list of
                    # subordinate records
                    staging_record_key = generate_staging_record_key(lineitem_record.source_record['order_id'])
                    order_elements = redisx.Set(staging_record_key, redis_svc.redis_server)
                    key = couchbase_svc.data_manager.insert_record(lineitem_record)
                    order_elements.add(key)
                
                    num_records_imported += 1
                    if num_records_imported % 100000 == 0:
                        log.info('+++ %d %s records imported.' % (num_records_imported, record_type_name))
                except Exception, err:
                    log.error('%s thrown while processing line %d of export file %s: %s' % (err.__class__.__name__, linenum, filename, err.message))
                finally:
                    linenum += 1

    log.info('+++ %d %s records imported.' % (num_records_imported, record_type_name))
    log.info('import of %s complete.' % record_type_name)



def get_lineitems_for_order(order_id, couchbase_svc, redis_svc, **kwargs):
    results = {}
    for lineitem_record_type in order_components:
        results[lineitem_record_type] = []
        
    key = generate_staging_record_key(order_id)
    lineitem_set = redisx.Set(key, redis_svc.redis_server)
    for lineitem_key in lineitem_set.members():
        lineitem_record = couchbase_svc.data_manager.lookup_record_raw(lineitem_key)
        lineitem_record_type = lineitem_record['record_type']
        results[lineitem_record_type].append(lineitem_record['source_record'])        

    return results



def assemble_order_records(csv_filenames, couchbase_svc, redis_svc, raw_record_id_queue, segment_counter, **kwargs):
    log = logging.getLogger(LOG_TAG)        
    record_type_name = 'orders'
    mapbuilder_func = mapbuilder_func_table[record_type_name]

    log.info('### Assembling order records in Couchbase...')
    
    num_records_imported = 0    
    csv_record_map = mapbuilder_func()    
    for filename in csv_filenames:

        log.info('reading order records from file %s' % filename)
        with open(filename, "r") as f:
            for line in f:                
                rec = csv_record_map.row_to_dictionary(line)
                order_lineitems = get_lineitems_for_order(rec['order_id'], couchbase_svc, redis_svc)
                rec.update(order_lineitems)
                
                target_rec = { 'status': const.STATUS_UNPROCESSED,
                               'record_type': const.RECTYPE_ORDER,
                               'segment': segment_counter.next_segment(),
                               'timestamp': datetime.datetime.now().isoformat(),
                               'source_record': rec
                }
            
                key = couchbase_svc.insert_record(const.RECTYPE_ORDER, target_rec)
                raw_record_id_queue.push(key)
                num_records_imported += 1
                if num_records_imported % 100000 == 0:
                    log.info('+++ %d order records saved.' % (num_records_imported))

    log.info('order record assembly complete.')
    return num_records_imported


    
class SegmentCounter(object):
    def __init__(self, segment_array):
        if not len(segment_array):
            raise Exception('Attempted to initialize a SegmentCounter using a zero-length array.')
        self.segment_array = segment_array
        self.counter = 0


    def next_segment(self):
        result = self.segment_array[self.counter]
        self._increment()
        return result

    
    def _increment(self):
        if self.counter < len(self.segment_array) - 1:
            self.counter = self.counter + 1
        else:
            self.counter = 0
            


def main(args):
    windowed_export_mode = False
    num_reading_frames = int(args.get('<num_frames>', -1))
    reading_frame_size = int(args.get('<framesize>', -1))
    s3_bucket_name = args.get('<s3_bucket>')
    paging_context = None
    if num_reading_frames > 0 and reading_frame_size > 0:
        windowed_export_mode = True        
        paging_context = tdx.PagingContext(num_reading_frames, reading_frame_size)

    init_filename = args['<initfile>']
    yaml_config = common.read_config_file(init_filename)
    num_segments = int(yaml_config['globals'].get('num_segments', DEFAULT_SEGMENT_COUNT))
    pipeline_id = yaml_config['globals']['pipeline_id']

    log_directory = yaml_config['globals']['log_directory']
    job_id = tdx.generate_job_id('extract', pipeline_id)
    log_filename = tdx.generate_logfile_name(job_id)
    logger = tdx.init_logging(LOG_TAG, os.path.join(log_directory, log_filename), logging.DEBUG)

    print '%s script started at %s, logging to %s...' % (LOG_TAG, datetime.datetime.now().isoformat(), log_filename)
    
    service_objects = snap.initialize_services(yaml_config, logger)
    so_registry = common.ServiceObjectRegistry(service_objects)
    redis_svc = so_registry.lookup('redis')
    raw_record_id_queue = redis_svc.get_raw_record_queue(pipeline_id)
    
    couchbase = so_registry.lookup('couchbase')
    
    redshift_svc = so_registry.lookup('redshift')
    password = os.getenv('REDSHIFT_READONLY_PASSWORD')
    redshift_svc.login(password)

    pipeline_info_mgr = tdx.PipelineInfoManager(pipeline_id,                                                
                                                couchbase.journal_manager)

    # TODO: add journaling
    '''
    start_log = OpLogEntry().add_field(TimestampField()).add_field(PIDField()).add_field(RecordPageField(my_reading_frame))
    end_log = OpLogEntry().add_field(TimestampField()).add_field(PIDField()).add_field(RecordPageField(my_reading_frame))
    with journal('my_log', pipeline_info_mgr.oplog(), start_log, end_log):
    '''

    job_dbkey = pipeline_info_mgr.record_job_start(job_id, 'placeholder for command line used to launch this job')
    with redshift_svc.get_connection() as conn:
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        rsec = tdx.RedshiftS3Context(s3_bucket_name, aws_access_key_id, aws_secret_key)
        
        orders_export_filemap = {}
        lineitems_export_filemap = {}
        
        # export the lineitems which constitute an order
        for lineitem_type in order_components:            
            lineitems_export_filemap.update(export_redshift_records_to_csv(lineitem_type, rsec, conn, paging_context, pipeline_mgr = pipeline_info_mgr))
            
        # export the order records themselves        
        orders_export_filemap.update(export_redshift_records_to_csv('orders', rsec, conn, paging_context, pipeline_mgr = pipeline_info_mgr))
        

    start_time = time.time()
    logger.info('starting csv import to Redshift...')
    # pull the component lineitem records into couchbase
    # note that the records will not be segmented (they will all have a segment number of -1)
    for record_type_name, csv_filenames in lineitems_export_filemap.iteritems():
        import_csv_records_to_couchbase(record_type_name, csv_filenames, couchbase, redis_svc, SegmentCounter([-1]), pipeline_mgr = pipeline_info_mgr)
   
    # now assemble subrecords from previously imported data

    logger.info('csv import complete.')
    logger.info('orders filemap: %s' % tdx.jsonpretty(json.dumps(orders_export_filemap)))
    logger.info('lineitems filemap: %s' % tdx.jsonpretty(json.dumps(lineitems_export_filemap)))
    logger.info('assembling records in Couchbase...')
        
    num_orders_imported = assemble_order_records(orders_export_filemap['orders'],
                                                 couchbase,
                                                 redis_svc,
                                                 raw_record_id_queue,
                                                 SegmentCounter(range(0, num_segments)),
                                                 pipeline_mgr = pipeline_info_mgr)
    
    end_time = time.time()
    logger.info('%d order records imported.' % num_orders_imported)
    logger.info('Total running time %s' % (tdx.hms_string(end_time - start_time)))
    pipeline_info_mgr.record_job_end(job_dbkey)
    
    print 'exiting.'
    

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

