#!/usr/bin/env python


import sqldbx
import couchbasedbx as cbx
import redisx
import telegraf

from snap import snap
from snap import common
import constants as const

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy_utils import UUIDType
import uuid

Base = declarative_base()

import boto3
import os


class IngestServiceObject():
    def __init__(self, logger, **kwargs):
        logger.info('>>> Initializing Praxis data ingest service with params: %s' % (kwargs))
        self.log = logger
        self.input_dir = kwargs['input_dir']
        self.output_bucket = kwargs['output_bucket']
        self.write_promise_queues = []
        # TODO: factor out magic IP addresses to the Praxis init file
        knodes = []
        knodes.append(telegraf.KafkaNode('172.31.17.24'))
        knodes.append(telegraf.KafkaNode('172.31.18.160'))
        knodes.append(telegraf.KafkaNode('172.31.17.250'))
        self.kafka_ingest_log = telegraf.KafkaIngestRecordWriter(knodes)

        # TODO: plug in different error handler
        dsn = 'https://64488b5074a94219ba25882145864700:9129da74c26a43cd84760d098b902f97@sentry.io/163031'
        self.error_handler = telegraf.SentryErrorHandler(dsn)


    def write_ingest_record(self, record_dict, topic):
        return self.kafka_ingest_log.write(topic, record_dict)


    def sync_ingest_records(self):
        # TODO: factor out magic timeout value to init file
        self.kafka_ingest_log.sync(0.1)


    def process_write_futures(self, write_futures):
        kafka_promise_queue = telegraf.IngestWritePromiseQueue(self.error_handler,
                                                               self.log,
                                                               write_futures,
                                                               debug_mode=True)
        # self.write_promise_queues.append(kafka_promise_queue)
        kafka_promise_queue.start()


    def create_ingest_record(self, record_type, stream_name, **kwargs):
        header = telegraf.IngestRecordHeader(record_type, stream_name, kwargs)
        msg_builder = telegraf.IngestRecordBuilder(header)
        for key, value in kwargs.iteritems():
            msg_builder.add_field(key, value)

        return msg_builder.build()


    def generate_error_filename(self, client_id, s3_object_key):
        # TODO: un-hardcode this so that we generate unique filenames
        return '/tmp/%s_errors.txt' % client_id


    def read_msg(self, input_data, **kwargs):
        self.log.info(common.jsonpretty(input_data))
        if input_data.get('Records'):
            self.log.info('### S3 bucket name: %s' % input_data['Records'][0]['s3']['bucket']['name'])
            self.log.info('### new S3 object: %s' % input_data['Records'][0]['s3']['object']['key'])
            return dict(input_data['Records'][0]['s3'])
        return {}


    def check_line(self, line, validation_profile):
        checks_remaining = 2
        if not validation_profile.text_qualifier:
            checks_remaining -= 1

        num_fields_in_record = len(line.split(validation_profile.delimiter))
        if num_fields_in_record == validation_profile.num_fields:
            checks_remaining -= 1

        if checks_remaining:
            return False
        return True


    def check_file(self, filename, validation_profile, **kwargs):
        bad_lines = []
        good_lines = []

        self.log.info('validating local file %s...' % (filename))
        with open(filename, 'r') as f:
            f.readline()  # header
            line_number = 1
            while True:
                current_line = f.readline()
                if current_line:
                    status = self.check_line(current_line.strip(), validation_profile)
                    if not status:
                        bad_lines.append({'linenum': line_number, 'data': current_line})
                    else:
                        good_lines.append(current_line)
                    line_number += 1
                else:
                    break

        self.log.info('%d lines read from file %s.' % (line_number, filename))
        self.log.info('%d bad records found.' % len(bad_lines))

        return {'good_lines': good_lines, 'bad_records': bad_lines}




class S3Key(object):
    def __init__(self, s3_key_string):
        self.folder_path = self.extract_folder_path(s3_key_string)
        self.object_name = self.extract_object_name(s3_key_string)
        self.full_name = s3_key_string


    def extract_folder_path(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return ''
        key_tokens = s3_key_string.split('/')
        return '/'.join(key_tokens[0:-1])


    def extract_object_name(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return s3_key_string
        return s3_key_string.split('/')[-1]

class S3ServiceObject():
    def __init__(self, log, **kwargs):
        log.info('>>> Initializing Praxis S3 service object')
        self.log = log
        self.local_tmp_path = kwargs['local_temp_path']
        self.s3client = boto3.client('s3')


    def upload_object(self, local_filename, bucket_name, bucket_path=None):
        s3_key = None
        with open(local_filename, 'rb') as data:
            base_filename = os.path.basename(local_filename)
            if bucket_path:
                s3_key = os.path.join(bucket_path, base_filename)
            else:
                s3_key = base_filename
            self.s3client.upload_fileobj(data, bucket_name, s3_key)
        return s3_key


    def download_object(self, bucket_name, s3_key_string):
        s3_path_header = 's3://%s' % bucket_name
        s3_object_key = S3Key(s3_key_string)
        local_filename = os.path.join(self.local_tmp_path, s3_object_key.object_name)
        with open(local_filename, "wb") as f:
            self.s3client.download_fileobj(bucket_name, s3_object_key.full_name, f)

        return local_filename



class PostgresServiceObject(object):
    def __init__(self, logger, **kwargs):
        self.log = logger
        self.host = kwargs['host']
        self.port = int(kwargs.get('port', 5432))
        self.username = kwargs['username']
        self.schema = kwargs['schema']
        self.db_name = kwargs['database']
        self.db = sqldbx.PostgreSQLDatabase(self.host, self.db_name, self.port)
        self.db.login(self.username, kwargs['password'])
        self._data_manager = sqldbx.PersistenceManager(self.db)


    @property
    def data_manager(self):
        return self._data_manager

    @property
    def database(self):
        return self.db


class MSSQLServiceObject(object):
    def __init__(self, logger, **kwargs):
        kwreader = common.KeywordArgReader('host', 'username', 'database', 'password')
        kwreader.read(**kwargs)

        self.log = logger
        self.host = kwreader.get_value('host')
        self.port = int(kwreader.get_value('port') or 1433)
        self.username = kwreader.get_value('username')
        self.db_name = kwreader.get_value('database')
        self.password = kwreader.get_value('password')
        self.db = sqldbx.SQLServerDatabase(self.host, self.db_name, self.port)
        self.db.login(self.username, self.password)
        self._data_manager = sqldbx.PersistenceManager(self.db)


    @property
    def data_manager(self):
        return self._data_manager

    @property
    def database(self):
        return self.db


class RedshiftServiceObject():
    def __init__(self, logger, **kwargs):
        logger.info('>>> initializing RedshiftServiceObject with params: %s' % (kwargs))

        self.host = kwargs['host']
        self.db_name = kwargs['db_name']
        self.port = kwargs['port']
        self.username = kwargs['username']
        self.schema = kwargs['schema']
        self.data_manager = None
        self.db = sqldbx.PostgreSQLDatabase(self.host, self.db_name, self.port)


    def login(self, password):
        self.db.login(self.username, password, self.schema)
        self.data_manager = sqldbx.PersistenceManager(self.db)


    def get_connection(self):
        return self.db.engine.connect()



class CouchbaseServiceObject():
    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.host = kwargs['host']
        self.data_bucket_name = kwargs['data_bucket_name']
        self.journal_bucket_name = kwargs['journal_bucket_name']
        self.cache_bucket_name = kwargs['cache_bucket_name']

        self.couchbase_server = cbx.CouchbaseServer(self.host)
        self.data_manager = cbx.CouchbasePersistenceManager(self.couchbase_server, self.data_bucket_name)
        self.journal_manager = cbx.CouchbasePersistenceManager(self.couchbase_server, self.journal_bucket_name)
        self.cache_manager = cbx.CouchbasePersistenceManager(self.couchbase_server, self.cache_bucket_name)


    def insert_record(self, record_type_name, record_dict):
        cb_record = cbx.CouchbaseRecordBuilder(record_type_name).add_fields(record_dict).build()
        return self.data_manager.insert_record(cb_record)


class RedisServiceObject():
    def __init__(self, logger, **kwargs):
        self.log = logger
        self.host = kwargs['host']
        self.port = kwargs.get('port', 6379)
        self.redis_server = redisx.RedisServer(self.host, self.port)
        self.transformed_record_queue_name = kwargs['transformed_record_queue_name']
        self.raw_record_queue_name = kwargs['raw_record_queue_name']
        self.generator_to_user_map_name = kwargs['generator_user_map_name']


    def get_transformed_record_queue(self, pipeline_id):
        key = redisx.compose_key(pipeline_id, self.transformed_record_queue_name)
        self.log.info('generated redis key for transformed record queue: "%s"' % key)
        return redisx.Queue(key, self.redis_server)


    def get_raw_record_queue(self, pipeline_id):
        key = redisx.compose_key(pipeline_id, self.raw_record_queue_name)
        self.log.info('generated redis key for raw record queue: "%s"' % key)
        return redisx.Queue(key, self.redis_server)


    def get_generator_to_user_map(self, pipeline_id):
        key = redisx.compose_key(pipeline_id, self.generator_to_user_map_name)
        self.log.info('generated redis key for generator-to-user map: "%s"' % key)
        return redisx.Hashtable(key, self.redis_server)
