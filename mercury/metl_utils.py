#!/usr/bin/env python

import os
import logging
import couchbasedbx as cbx
import journaling
import datetime
import boto3
import json
import getpass
import constants as const
import tempfile
import logging


s3_export_prefix = 'tdx_%s' % datetime.datetime.now().isoformat()


# a-zA-Z0-9 except ambiguous characters like 0/O and 1/l:
id_alphabet = "23456789abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
id_alphabet_map = dict((c, v) for v, c in enumerate(id_alphabet))
id_base = len(id_alphabet)


def init_logging(tag, filename, log_level):
    logger = logging.getLogger(tag)
    logger.setLevel(log_level)
    ch = logging.FileHandler(filename)
    ch.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def find_nth(string, substring, n):
    start = string.find(substring)
    while start >= 0 and n > 1:
        start = string.find(substring, start+len(substring))
        n -= 1
    return start


def hms_string(sec_elapsed):
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60.
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)




# This section is courtesy of Jens Wirf  ========================================

def internal_to_external(id):
    """Given a positive integer in base 10, returns an alphanumeric version"""
    if id < 1:
        raise ValueError("Can only encode natural numbers")

    ret = ''
    while id != 0:
        ret = id_alphabet[id % id_base] + ret
        id = id / id_base
    return ret


def external_to_internal(external_id):
    """Given an alphanumeric ID, returns the same in base 10 (normal number)"""
    try:
        ret = 0
        for i, c in enumerate(external_id[::-1]):
            ret += (id_base ** i) * id_alphabet_map[c]
    except KeyError:
        raise ValueError("'%s' was not encoded by this method!" % external_id)
    return ret


def csv_to_list(values):
    """Given an comma separated list, return it as a `list()`"""
    return (values or '').split(',')


def csv_external_to_internal(external_ids):
    """Given an comma separated alphanumeric ID list, returns the same in base 10"""
    return [external_to_internal(external_id) for external_id in csv_to_list(external_ids)]

#==============================================================================


def redshift_console_login(redshift_service_obj):
    prompt = 'Connecting to database host: %s, db: %s as user: %s.' % (redshift_service_obj.host, redshift_service_obj.port, redshift_service_obj.username)    
    print(prompt)
    password = getpass.getpass()
    redshift_service_obj.login(password)

    
def jsonpretty(dict):
    return json.dumps(dict, indent=4, sort_keys=True)

        
def db_record_to_dictionary(record):
    result = {}
    for key in record.iterkeys():       
        if record[key].__class__.__name__ == 'datetime':
            result[key] = record[key].isoformat()
        elif record[key].__class__.__name__ == 'float':
            result[key] = int(record[key] * 100)
        else:
            result[key] = record[key]
    return result


def generate_job_id(job_type_name, pipeline_id):
    datestring = datetime.datetime.now().isoformat().split('T')[0]
    return 'jobid_%s_%s.%s.%s' % (pipeline_id, job_type_name.lower(), datestring, os.getpid())


def generate_logfile_name(job_id):
    return '%s.log' % job_id


def generate_order_key(record):
    return 'tdx_order_%s' % (record.source_record['order_id'])


def generate_transformed_order_key(record):
    return '%s_%s' % (const.RECTYPE_ORDER_TRANSFORMED, record.source_record['order_id'])


class JournalExtractRecord(cbx.CouchbaseRecord):
    def __init__(self, state, ):
        CouchbaseRecord.__init__('journal_extract')


class InvalidReadingFrameException(Exception):
    def __init__(self):
        Exception.__init__(self, 'Error: attempted to create a ReadingFrame with page size < 1  and/or index number < 0.')

        
class FrameShiftException(Exception):
    def __init__(self):
        Exception.__init__(self, 'Error: attempted to right-shift a ReadingFrame past index number 0.')

        
class ReadingFrame(object):
    def __init__(self, index_number, page_size):
        self.size = page_size
        self.index_number = index_number
        if index_number < 0 or page_size < 1:
            raise InvalidReadingFrameException()

        
    def shift_right(self):
        return ReadingFrame(self.index_number + 1, self.size)

    
    def shift_left(self):
        if self.index_number == 0:
            raise FrameShiftException(0)
        
        return ReadingFrame(self.index_number - 1, self.size)

            

class PagingContext(object):
    def __init__(self, num_frames_to_read, frame_size):
        self.frame_size = frame_size
        self.num_frames_to_read = num_frames_to_read

    def initial_reading_frame(self):
        return ReadingFrame(0, self.frame_size)



class RedshiftS3Context(object):
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_key):
        self.s3_bucket_name = bucket_name
        self.access_key_id = aws_access_key_id
        self.secret_key = aws_secret_key

        
    def s3_key(self, file_prefix):
        bucket_path = 's3://%s' % (self.s3_bucket_name)
        return '/'.join([bucket_path, file_prefix])


    def credentials(self):
        return 'aws_access_key_id=%s;aws_secret_access_key=%s' % (self.access_key_id, self.secret_key)
    
        
    def generate_unload_statement(self, query, output_file_prefix, **kwargs):
        return "UNLOAD ('%s') TO '%s' credentials '%s' %s;" % \
            (query, self.s3_key(output_file_prefix), self.credentials(), kwargs.get('options_string', ''))


    def generate_copy_statement_manifest(self, table_name, manifest_key, **kwargs):
        return "copy %s from '%s' credentials '%s' %s;" % (table_name, manifest_key, self.credentials(), "manifest delimiter ',';")


    def generate_copy_statement_datafile(self, table_name, column_list, datafile_s3_object_key, **kwargs):
        delimiter_char = kwargs.get('delimiter', ',')
        return "copy %s from '%s' credentials '%s' delimiter '%s';" % (table_name, datafile_s3_object_key, self.credentials(), delimiter_char)

    
    def export_records(self, db_connection, query, output_file_prefix, **kwargs):
        unload_statement = self.generate_unload_statement(query, output_file_prefix, **kwargs)        
        db_connection.execute(unload_statement)


    def import_records(self, db_connection, table_name, column_list, bucket_object_key, **kwargs):
        if kwargs.get('use_manifest', False):
            copy_stmt = self.generate_copy_statement_manifest(table_name, self.s3_key(bucket_object_key), **kwargs)
        else:            
            copy_stmt = self.generate_copy_statement_datafile(table_name, column_list, self.s3_key(bucket_object_key), **kwargs)

        tx = db_connection.begin()
        db_connection.execute(copy_stmt)
        tx.commit()
        return copy_stmt


    
class RedshiftS3Exporter(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

        self.last_op_query = 'select pg_last_query_id();'
        self.unload_list_query='''
            select query, path
            from stl_unload_log
            where query={0}
            order by path;
        '''


    def export_records_as_csv(self, export_query, export_file_prefix, redshift_export_context, paging_context=None, **kwargs):
        #log = logging.getLogger(LOG_TAG)
        delimiter_char = kwargs.get('delimiter', '|')
        unload_options = "delimiter '%s' escape manifest allowoverwrite" % delimiter_char        
        #log.debug('Exporting records from Redshift to CSV...')
        if paging_context:
            pages_remaining = paging_context.num_frames_to_read
            reading_frame = paging_context.initial_reading_frame()
        
            while pages_remaining:
                page_number = (paging_context.num_frames_to_read - pages_remaining) + 1
                record_id_lower_bound = reading_frame.index_number * reading_frame.size
                record_id_upper_bound = record_id_lower_bound + reading_frame.size - 1
                redshift_export_context.export_records(self.db_connection,                                            
                                                   export_query.format(record_id_lower_bound, record_id_upper_bound), 
                                                   '%s_page_%d_' % (export_file_prefix, page_number),
                                                       options_string=unload_options)
            
                reading_frame = reading_frame.shift_right()
                pages_remaining -= 1
        else:
            redshift_export_context.export_records(self.db_connection,                                            
                                                   export_query, 
                                                   export_file_prefix,
                                                   options_string=unload_options)


        resultset = self.db_connection.execute(self.last_op_query)
        data = list(resultset.fetchone())
        unload_query_id = -1
        if data:
            unload_query_id = data[0]
        else:
            raise Exception('Unable to retrieve the query ID for Redshift UNLOAD operation.')
        
        #log.debug('unload query ID: %d' % unload_query_id)

        s3_object_paths = []
        resultset = self.db_connection.execute(self.unload_list_query.format(unload_query_id))
        for row in resultset:
            s3_object_paths.append(row['path'].strip())


        manifest_path = 's3://%s/%smanifest' % (redshift_export_context.s3_bucket_name, export_file_prefix)
        #log.debug('manifest file is: %s' % manifest_path)

        return {'manifest': manifest_path, 'datafiles': s3_object_paths}
        


    def download_s3_object(self,  s3_object_path, redshift_export_context):
        s3_path_header = 's3://%s' % redshift_export_context.s3_bucket_name
        s3_object_key = s3_object_path.split(s3_path_header)[1].lstrip('/')
        local_filename = s3_object_key.replace('/', '.')
        fileobj_tuple = tempfile.mkstemp(local_filename)
        
        #log.debug('Downloading %s...' % fileobj_tuple[1])
        s3 = boto3.client('s3')
        with open(fileobj_tuple[1], "wb") as f:
            s3.download_fileobj(redshift_export_context.s3_bucket_name, s3_object_key, f)

        return fileobj_tuple[1]
        

    

class PipelineInfoManager(object):
    def __init__(self, pipeline_id, couchbase_persistence_mgr):
        self.id = pipeline_id
        self.persistence_mgr = couchbase_persistence_mgr
        self._oplog_writer = journaling.CouchbaseOpLogWriter(const.RECTYPE_OPERATION, couchbase_persistence_mgr)


    def generate_job_key(self, job_record):
        return 'tdx_%s' % job_record.id


    def record_job_start(self, job_id_string, command_data):
        data = {}        
        data['pipeline_id'] = self.id
        data['id'] = job_id_string
        data['command'] = command_data
        data['start_time'] = datetime.datetime.now().isoformat()
        data['end_time'] = None
        data['pid'] = os.getpid()

        self.persistence_mgr.register_keygen_function(const.RECTYPE_JOB, self.generate_job_key)
        job_record = cbx.CouchbaseRecordBuilder(const.RECTYPE_JOB).add_fields(data).build()
        return self.persistence_mgr.insert_record(job_record)

    
    def record_job_end(self, job_key):
        record = self.persistence_mgr.lookup_record(const.RECTYPE_JOB, job_key)
        if not record:
            raise Exception('No %s record with key %s found.' % (const.RECTYPE_JOB, job_key))

        record.end_time = datetime.datetime.now().isoformat()
        self.persistence_mgr.update_record(record, job_key, True) # we pass true because the record must exist


    def oplog_writer(self):
        return self._oplog_writer


                        
