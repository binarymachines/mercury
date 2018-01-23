#!/usr/bin/env python


import sqldbx
import couchbasedbx as cbx
import redisx



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
