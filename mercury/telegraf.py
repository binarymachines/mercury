#!/usr/bin/env python

import os
import sys
import threading
import time
import datetime
import json
import docopt
import yaml
from snap import common
from mercury import sqldbx as sqlx

import logging
import copy
from kafka import KafkaProducer, KafkaConsumer, KafkaClient
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from kafka.common import OffsetRequestPayload

from sqlalchemy import Integer, String, DateTime, text, and_
from sqlalchemy import Integer, String, DateTime, Float, text
from sqlalchemy.sql import bindparam

from raven import Client
from raven.handlers.logging import SentryHandler

from logging import Formatter
#import traceback
from sqlalchemy.sql import bindparam


log = logging.getLogger(__name__)
DEFAULT_SENTRY_DSN = 'https://64488b5074a94219ba25882145864700:9129da74c26a43cd84760d098b902f97@sentry.io/163031'



class NoSuchPartitionException(Exception):
    def __init__(self, partition_id):
        Exception.__init__(self, 'The target Kafka cluster has no partition "%s".' % partition_id)


class UnregisteredTransferFunctionException(Exception):
    def __init__(self, op_name):
        Exception.__init__(self, 'No transfer function registered under the name "%s" with transfer agent.' % op_name)


class TelegrafErrorHandler(object):
    def __init__(self, logging_level=logging.DEBUG, sentry_dsn=DEFAULT_SENTRY_DSN):
        self._client = Client(sentry_dsn)
        sentry_handler = SentryHandler()
        sentry_handler.setLevel(logging_level)
        log.addHandler(sentry_handler)


    @property
    def sentry(self):
        return self._sentry_logger



class IngestRecordHeader(object):
    def __init__(self, **kwargs):

        kwreader = common.KeywordArgReader('record_type', 'pipeline_id')
        kwreader.read(**kwargs)

        self._version = 1
        self._record_type = kwreader.get_value('record_type')
        self._pipeline_id = kwreader.get_value('pipeline_id')        
        self._timestamp = datetime.datetime.now().isoformat()
        self._extra_headers = []
        for key, value in kwargs.items():
            if key not in ['record_type', 'pipeline_id']:
                self._extra_headers.append({'name': key, 'value': value})


    def data(self):
        result = {}
        result['version'] = self._version
        result['record_type'] = self._record_type
        result['pipeline_id'] = self._pipeline_id
        result['ingest_timestamp'] = self._timestamp
        result['extra_headers'] = self._extra_headers
        return result



class IngestRecordBuilder(object):
    def __init__(self, record_header, **kwargs):
        self.header = record_header
        self.source_data = kwargs or {}


    def add_field(self, name, value):
        self.source_data[name] = value
        return self


    def add_fields(self, **kwargs):
        self.source_data.update(kwargs)
        return self


    def build(self):
        result = {}
        result['header'] = self.header.data()
        result['body'] = self.source_data
        return result



class KafkaNode(object):
    def __init__(self, host, port=9092):
        self._host = host
        self._port = port

    def __call__(self):
        return '%s:%s' % (self._host, self._port)


def json_serializer(value):
    return json.dumps(value).encode('utf-8')


def json_deserializer(value):
    return json.loads(value.decode('utf-8'))


class KafkaMessageHeader(object):
    def __init__(self, header_dict):
        self.__dict__ = header_dict


class KafkaCluster(object):
    def __init__(self, nodes=[], **kwargs):
        self._nodes = nodes


    def add_node(self, kafka_node):
        new_nodes = copy.deepcopy(self._nodes)
        new_nodes.append(kafka_node)
        return KafkaCluster(new_nodes)


    @property
    def nodes(self):
        return ','.join([n() for n in self._nodes])


    @property
    def node_array(self):
        return self._nodes



class KafkaOffsetManagementContext(object):
    def __init__(self, kafka_cluster, topic, **kwargs):
        #NOTE: keep this code for now
        '''
        self._client = KafkaClient(bootstrap_servers=kafka_cluster.nodes)
        self._metadata = self._client.cluster
        '''
        self._partition_table = {}

        consumer_group = kwargs.get('consumer_group', 'test_group')
        kreader = KafkaIngestRecordReader(topic, kafka_cluster.node_array, consumer_group)

        print('### partitions for topic %s: %s' % (topic, kreader.consumer.partitions))
        part1 = kreader.consumer.partitions[0]
        print('### last committed offset for first partition: %s' % kreader.consumer.committed(part1))


    @property
    def partitions(self):
        # TODO: figure out how to do this
        return None


    def get_offset_for_partition(self, partition_id):
        '''NOTE TO SARAH: this should stay more or less as-is, because
        however we retrieve the partition & offset data from the cluster,
        we should store it in a dictionary'''

        offset = self._partition_table.get(partition_id)
        if offset is None:
            raise NoSuchPartitionException(partition_id)
        return offset



class KafkaLoader(object):
    def __init__(self, topic, kafka_ingest_record_writer, **kwargs):
        self._topic = topic
        self._kwriter = kafka_ingest_record_writer
        self._header = IngestRecordHeader(**kwargs)


    def load(self, data):
        msg_builder = IngestRecordBuilder(self._header)
        for key, value in data.items():
            msg_builder.add_field(key, value)
        ingest_record = msg_builder.build()

        log.debug('writing record to Kafka topic "%s":' % self._topic)
        log.debug(ingest_record)
        self._kwriter.write(self._topic, ingest_record)



class KafkaIngestRecordWriter(object):
    def __init__(self, kafka_node_array, serializer=json_serializer):
        self.producer = KafkaProducer(bootstrap_servers=','.join([n() for n in kafka_node_array]),
                                      value_serializer=serializer,
                                      acks=1,
                                      api_version=(0,10))

        error_handler = ConsoleErrorHandler()
        self._promise_queue = IngestWritePromiseQueue(error_handler, debug_mode=True)


    def write(self, topic, ingest_record):
        future = self.producer.send(topic, ingest_record)
        self._promise_queue.append(future)
        return future


    def sync(self, timeout=0):
        self.producer.flush(timeout or None)


    @property
    def promise_queue_size(self):
        return self._promise_queue.size


    def process_write_promise_queue(self):
        self._promise_queue.run()
        return self._promise_queue.errors



class CheckpointTimer(threading.Thread):
    def __init__(self, checkpoint_function, **kwargs):
        threading.Thread.__init__(self)
        kwreader = common.KeywordArgReader('checkpoint_interval').read(**kwargs)
        self._seconds = 0
        self._stopped = True
        self._checkpoint_function = checkpoint_function
        self._interval = kwreader.get_value('checkpoint_interval')
        self._checkpoint_function_args = kwargs

        
    def run(self):        
        self._stopped = False
        log.info('starting checkpoint timer at %s.' % datetime.datetime.now().isoformat())
        while not self._stopped:
            time.sleep(1)
            self._seconds += 1
            if self. _seconds >= self._interval:
                self._checkpoint_function(**self._checkpoint_function_args)
                self.reset()


    @property
    def elapsed_time(self):
        return self._seconds


    def reset(self):        
        self._seconds = 0
        log.info('resetting checkpoint timer at %s.' % datetime.datetime.now().isoformat())


    def stop(self):
        self._stopped = True
        log.info('stopping checkpoint timer at %s.' % datetime.datetime.now().isoformat())



def filter(filter_function, records, **kwargs):
    return (rec for rec in records if filter_function(rec, **kwargs))


def file_generator(**kwargs):
    filename = kwargs.get('filename')
    with open(filename) as f:
        while True:
            line = f.readline()
            if line:
                yield line
            else:
                return


class RecordSource(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('generator', 'services')
        kwreader.read(**kwargs)
        self._generator = kwargs['generator']
        self._services = kwargs['services']


    def records(self, **kwargs):
        kwargs.update({'services': self._services})
        data = self._generator(**kwargs)
        filter_func = kwargs.get('filter')
        if filter_func:
            return (filter(filter_func, data, **kwargs))
        else:
            return (record for record in data)


class TopicFork(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('accept_topic',
                                           'reject_topic',
                                           'qualifier',
                                           'service_objects')
        kwreader.read(**kwargs)
        self.accept_topic = kwargs['accept_topic']
        self.reject_topic = kwargs['reject_topic']
        self.qualify = kwargs['qualifier']
        self.services = kwargs['services']


    def split(self, record_generator, **kwargs):
        kwreader = common.KeywordArgReader('kafka_writer')
        kwreader.read(**kwargs)
        kafka_writer = kwargs['kafka_writer']
        kwargs.update({'services': self.services})
        for record in record_generator:
            if self.qualify(record, **kwargs):
                kafka_writer.write(self.accept_topic, record)
            else:
                kafka_writer.write(self.reject_topic, record)


class TopicFilter(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('target_topic',
                                           'qualifier',
                                           'services')
        kwreader.read(**kwargs)
        self.target_topic = kwargs['target_topic']
        self.qualify = kwargs['qualifier']
        self.services = kwargs['services']


    def process(self, record_generator, **kwargs):
        kwreader = common.KeywordArgReader('kafka_writer')
        kwreader.read(**kwargs)
        kafka_writer = kwargs['kafka_writer']
        kwargs.update({'services': self.services})
        for record in record_generator:
            if self.qualify(record, **kwargs):
                kafka_writer.write(self.target_topic, record)


class TopicRouter(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('services')
        kwreader.read(**kwargs)
        self.services = kwargs['services']
        self.targets = {}


    def register_target(self, qualifier_function, target_topic, precedence=0):
        self.targets[(precedence, qualifier_function)] = target_topic

        # The default behavior is to test each record against each qualifier in
        # self.targets. In some cases it may be helpful to attempt matches in a definite order.
        # We do this by registering higher precedence numbers to the qualifiers we wish to call first.


    def process(self,
                record_generator,
                respect_precedence_order=False,
                allow_multi_target_match=True,
                **kwargs):
        if not self.targets:
            raise Exception('Empty target table. You must register at least one target in order to process records.')

        kwreader = common.KeywordArgReader('kafka_writer')
        kwreader.read(**kwargs)
        kafka_writer = kwargs['kafka_writer']
        default_topic = kwargs.get('default_topic')
        kwargs.update({'services': self.services})

        for record in record_generator:

            # The default behavior is that one record may be dispatched to multiple target topics if it passes
            # multiple qualifiers. If we don't want to test a record against all registered qualifiers, but instead break
            # after the first match, then we set the allow_multi_target_match param to False. Then we will
            # break out of the inner loop as soon as a qualifier returns True, or after all qualifiers
            # have returned False.
            #
            # If the respect_precedence_order param is True, the record will be tested against qualifiers
            # in descending order of precedence.

            if respect_precedence_order:
                qualifier_tuples = sorted(self.targets.keys(), reverse=True)
            else:
                qualifier_tuples = self.targets.keys()

            for key in qualifier_tuples:
                qualifier_func = key[1]
                topic = self.targets[key]
                matching_target_found = False
                if qualifier_func(record, **kwargs):
                    matching_target_found = True
                    kafka_writer.write(topic, record)
                    if not allow_multi_target_match:
                        break

            if not matching_target_found and default_topic:
                kafka_writer.write(default_topic, record)


class KafkaIngestRecordReader(object):
    def __init__(self,
                 topic,
                 *kafka_nodes,
                 **kwargs):

        self._topic = topic
        # commit on every received message by default
        self._commit_interval = kwargs.get('commit_interval', 1)
        deserializer = kwargs.get('deserializer', json_deserializer)
        group = kwargs.get('group', None)
        self._consumer = KafkaConsumer(group_id=group,
                                       bootstrap_servers=','.join([n() for n in kafka_nodes]),
                                       value_deserializer=deserializer,
                                       auto_offset_reset='earliest',
                                       consumer_timeout_ms=5000)


    def read(self, data_relay, **kwargs): # insist on passing a checkpoint_frequency as kwarg?

        # if we are in checkpoint mode, issue a checkpoint signal every <interval> seconds
        # or every <frequency> records, whichever is shorter
        checkpoint_frequency = int(kwargs.get('checkpoint_frequency', -1))
        checkpoint_interval = int(kwargs.get('checkpoint_interval', 300))

        checkpoint_mode = False
        checkpoint_timer = None
        if checkpoint_frequency > 0:
            checkpoint_mode = True
            checkpoint_timer = CheckpointTimer(data_relay.checkpoint, **kwargs)
            checkpoint_timer.start()

        message_counter = 0
        num_commits = 0
        error_count = 0

        for message in self._consumer:
            try:
                data_relay.send(message)
                if message_counter % self._commit_interval == 0:
                    self._consumer.commit()
                    num_commits += 1
                if checkpoint_mode and message_counter % checkpoint_frequency == 0:
                    data_relay.checkpoint(**kwargs)
                    checkpoint_timer.reset()
            except Exception as err:
                log.debug('Kafka message reader threw an exception from its DataRelay while processing message %d: %s' % (message_counter, str(err)))
                log.debug('Offending message: %s' % str(message))
                #traceback.print_exc()
                error_count += 1
            finally:
                message_counter += 1

        log.info('%d committed reads from topic %s, %d messages processed, %d errors.' % (num_commits, self._topic, message_counter, error_count))
        
        # issue a last checkpoint on our way out the door
        # TODO: find out if what's really needed here is a Kafka consumer
        # timeout exception handler

        if checkpoint_mode:
            try:
                log.info('Kafka message reader issuing final checkpoint command at %s...' % datetime.datetime.now().isoformat())
                data_relay.checkpoint(**kwargs)
            except Exception as err:
                log.error('Final checkpoint command threw an exception: %s' % str(err))
            finally:    
                checkpoint_timer.stop()

        return num_commits


    @property
    def commit_interval(self):
        return self._commit_interval


    @property
    def num_commits_issued(self):
        return self._num_commits


    @property
    def consumer(self):
        return self._consumer


    @property
    def topic(self):
        return self._topic



class DataRelay(object):
    def __init__(self, **kwargs):
        self._transformer = kwargs.get('transformer')


    def pre_send(self, src_message_header, **kwargs):
        pass


    def post_send(self, src_message_header, **kwargs):
        pass


    def _send(self, src_message_header, data, **kwargs):
        '''Override in subclass
        '''
        pass


    def _checkpoint(self, **kwargs):
        '''Override in subclass'''
        pass


    def checkpoint(self, **kwargs):
        try:
            self._checkpoint(**kwargs)
            log.info('data relay passed checkpoint at %s' % datetime.datetime.now().isoformat())
        except Exception as err:
            log.error('data relay failed checkpoint with error:')
            log.error(err)


    def send(self, kafka_message, **kwargs):
        header_data = {}
        header_data['topic'] = kafka_message.topic
        header_data['partition'] = kafka_message.partition
        header_data['offset'] = kafka_message.offset
        header_data['key'] = kafka_message.key

        kmsg_header = KafkaMessageHeader(header_data)
        self.pre_send(kmsg_header, **kwargs)
        if self._transformer:
            data_to_send = self._transformer.transform(kafka_message.value['body'])
            print('## Data to send: %s \n\n' % str(data_to_send))
        else:
            data_to_send = kafka_message.value['body']
        self._send(kmsg_header, data_to_send, **kwargs)
        self.post_send(kmsg_header, **kwargs)


class ConsoleRelay(DataRelay):
    def __init__(self, **kwargs):
        DataRelay.__init__(self, **kwargs)


    def _send(self, src_message_header, message_data):
        print('### record at offset %d: %s' % (src_message_header.offset, message_data))



class ObjectstoreDBRelay(DataRelay):
    def __init__(self, **kwargs):
        DataRelay.__init__(self, **kwargs)
        kwreader = common.KeywordArgReader('db', 'tablespec')
        kwreader.read(**kwargs)
        self.database = kwreader.get_value('db')        
        self.tablespec = kwreader.get_value('tablespec')
        self._insert_sql = text(self.tablespec.insert_statement_template)
        
        
    def _send(self, src_message_header, data, **kwargs):
        '''execute insert statement against objectstore DB'''

        rec_data = self.tablespec.convert_data(data)
        rec_data['generation'] = 0
        rec_data['correction_id'] = None
        insert_statement = self._insert_sql.bindparams(**rec_data)

        with sqlx.txn_scope(self.database) as session:
            session.execute(insert_statement)




class K2Relay(DataRelay):
    def __init__(self, target_topic, kafka_ingest_log_writer, **kwargs):
        DataRelay.__init__(self, **kwargs)
        self._target_log_writer = kafka_ingest_log_writer
        self._target_topic = target_topic


    def _send(self, kafka_message):
        self._target_log_writer.write(self._target_topic, kafka_message.value)



class BulkTransferAgent(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('local_temp_dir',
                                           'src_filename',
                                           'src_file_header',
                                           'src_file_delimiter')

        kwreader.read(kwargs)
        self._local_temp_directory = kwreader.get_value('local_temp_dir')
        self._source_filename = kwreader.get_value('src_filename')
        self._source_file_header = kwreader.get_value('src_file_header')
        self._source_file_delimiter = kwreader.get_value('src_file_delimiter')
        self._svc_registry = None
        self._transfer_functions = {}


    def register_service_objects(self, name, service_object_config):
        service_object_tbl = snap.initialize_services(service_object_config)
        self._svc_registry = common.ServiceObjectRegistry(service_object_tbl)


    def register_transfer_function(self, operation_name, transfer_function):
        self._transfer_functions[operation_name] = transfer_function


    def transfer(self, operation_name, **kwargs):
        transfer_func = self._transfer_functions.get(operation_name)
        if not transfer_func:
            raise UnregisteredTransferFunctionException(operation_name)

        transfer_func(self._svc_registry, **kwargs)


def dimension_id_lookup_func(value, dim_table_name, key_field_name, value_field_name, **kwargs):
    if not value:
        return None

    else:
        db_schema = 'olap'
        raw_template = """
            SELECT {field}
            from {schema}.{table}
            where {dim_table_value_field_name} = :source_value""".format(schema=db_schema,
                                                                         field=key_field_name,
                                                                         table=dim_table_name,
                                                                         dim_table_value_field_name=value_field_name)
        template = text(raw_template)
        stmt = template.bindparams(bindparam('source_value', String))

        data_mgr = kwargs['persistence_manager']
        dbconnection = data_mgr.database.engine.connect()
        # db_session = data_mgr.getSession()

        result = dbconnection.execute(stmt, {"source_value": value})
        record = result.fetchone()
        if not record:
            raise Exception('returned empty result set from query: %s where value is %s' % (str(stmt), value))

        return record[0]



class OLAPSchemaDimension(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('fact_table_field_name',
                                            'dim_table_name',
                                            'key_field_name',
                                            'value_field_name',
                                            'primary_key_type',
                                            'id_lookup_function')

        kwreader.read(**kwargs)

        self._fact_table_field_name = kwreader.get_value('fact_table_field_name')
        self._dim_table_name = kwreader.get_value('dim_table_name')
        self._key_field_name = kwreader.get_value('key_field_name')
        self._value_field_name = kwreader.get_value('value_field_name')
        self._pk_type = kwreader.get_value('primary_key_type')
        self._lookup_func = kwreader.get_value('id_lookup_function')


    @property
    def fact_table_field_name(self):
        return self._fact_table_field_name


    @property
    def primary_key_field_name(self):
        return self._key_field_name


    @property
    def primary_key_field_type(self):
        return self._pk_type


    @property
    def value_field_name(self):
        return self._value_field_name

    @property
    def value_field_type(self):
        return self._value_

    def lookup_id_for_value(self, value, **kwargs):
        return self._lookup_func(value,
                                 self._dim_table_name,
                                 self._key_field_name, 
                                 self._value_field_name, 
                                 **kwargs)



class OLAPSchemaFact(object):
    def __init__(self, table_name, pk_field_name, pk_field_type):
        self._table_name = table_name
        self._pk_field = pk_field_name
        self._pk_field_type = pk_field_type

    @property
    def table_name(self):
        return self._table_name

    @property
    def primary_key_field_name(self):
        return self._pk_field

    @property
    def primary_key_field_type(self):
        return self._pk_field_type



class NonDimensionField(object):
    def __init__(self, fname, ftype):
        self._field_name = fname
        self._field_type = ftype


    @property
    def field_name(self):
        return self._field_name


    @property
    def field_type(self):
        return self._field_type



class OLAPSchemaMappingContext(object):
    def __init__(self, schema_fact):
        self._fact = schema_fact
        self._dimensions = {}
        self._direct_mappings = {}

    @property
    def fact(self):
        return self._fact

    @property
    def dimension_names(self):
        return self._dimensions.keys()

    @property 
    def non_dimension_names(self):
        return self._direct_mappings.keys()


    def get_non_dimension_field(self, field_name):
        if not self._direct_mappings.get(field_name):
            #TODO: create custom exception
            raise Exception('No non-dimensional field "%s" registered with mapping context.' % field_name)
        return self._direct_mappings[field_name]
        
    
    def get_dimension(self, dimension_name):
        if not self._dimensions.get(dimension_name):
            #TODO: create custom exception
            raise Exception('No dimension "%s" registered with mapping context.' % dimension_name)
        return self._dimensions[dimension_name]


    def map_src_record_field_to_dimension(self, src_record_field_name, olap_schema_dimension):
        self._dimensions[src_record_field_name] = olap_schema_dimension


    def map_src_record_field_to_non_dimension(self, src_record_field_name, fact_field_name, fact_field_type):
        nd_field = NonDimensionField(fact_field_name, fact_field_type)
        self._direct_mappings[src_record_field_name] = nd_field


    def get_fact_values(self, source_record, **kwargs):
        data = {}
        #print('### source record info: %s'%  source_record)

        for src_record_field_name in self._direct_mappings.keys():
            non_dimension_field = self._direct_mappings[src_record_field_name]
            data[non_dimension_field.field_name] = source_record[src_record_field_name]

        for src_record_field_name in self._dimensions.keys():
            dimension = self._dimensions[src_record_field_name]
            data[dimension.fact_table_field_name] = dimension.lookup_id_for_value(source_record[src_record_field_name],
                                                                                  **kwargs)

        return data



class OLAPSchemaMappingContextBuilder(object):
    def __init__(self, yaml_config_filename, **kwargs):
        kwreader = common.KeywordArgReader('context_name')
        kwreader.read(**kwargs)
        self._context_name = kwreader.get_value('context_name')
        self._yaml_config = None
        with open(yaml_config_filename, 'r') as f:
            self._yaml_config = yaml.load(f)


    def load_sqltype_class(self, classname, **kwargs):
        pk_type_module = self._yaml_config['globals']['sql_datatype_module']
        klass = common.load_class(classname, pk_type_module)
        return klass


    def load_fact_pk_sqltype_class(self, classname, **kwargs):
        pk_type_module = self._yaml_config['globals']['primary_key_datatype_module']
        klass = common.load_class(classname, pk_type_module)
        return klass


    def load_fact_pk_type_options(self):
        #TODO: pull this from the YAML file
        return {'binary': False}


    def build(self):
        fact_config = self._yaml_config['mappings'][self._context_name]['fact']
        fact_table_name = fact_config['table_name']
        fact_pk_field_name = fact_config['primary_key_name']
        fact_pk_field_classname = fact_config['primary_key_type']


        fact_pk_field_class = self.load_fact_pk_sqltype_class(fact_pk_field_classname)
        pk_type_options = self.load_fact_pk_type_options()

        fact = OLAPSchemaFact(fact_table_name,
                              fact_pk_field_name,
                              fact_pk_field_class(**pk_type_options))

        mapping_context = OLAPSchemaMappingContext(fact)

        for dim_name in self._yaml_config['mappings'][self._context_name]['dimensions']:
            dim_config = self._yaml_config['mappings'][self._context_name]['dimensions'][dim_name]

            pk_type_class = self.load_sqltype_class(dim_config['primary_key_type'])

            dim = OLAPSchemaDimension(fact_table_field_name=dim_config['fact_table_field_name'],
                                      dim_table_name=dim_config['table_name'],
                                      key_field_name=dim_config['primary_key_field_name'],
                                      value_field_name=dim_config['value_field_name'],
                                      primary_key_type=pk_type_class(),
                                      id_lookup_function=dimension_id_lookup_func)

            source_record_fieldname = dim_name
            mapping_context.map_src_record_field_to_dimension(source_record_fieldname, dim)


        for non_dim_name in self._yaml_config['mappings'][self._context_name]['non_dimensions']:
            non_dim_config = self._yaml_config['mappings'][self._context_name]['non_dimensions'][non_dim_name]
            source_record_field = non_dim_name
            fact_field_name = non_dim_config['fact_field_name']
            fact_field_type = non_dim_config['fact_field_type']
            mapping_context.map_src_record_field_to_non_dimension(source_record_field,
                                                                  fact_field_name,
                                                                  self.load_sqltype_class(fact_field_type))

        return mapping_context
            

class OLAPSchemaDDLGenerator(object):
    fact_ddl_template = '''
    CREATE TABLE {schema}.{fact_table_name}
    (
        {pk_field_name} {pk_field_type} PRIMARY KEY NOT NULL,    
        {fields}    
    );
    '''

    dimension_ddl_template = '''
    CREATE TABLE {schema}.{dim_table_name}
    (
        {pk_field_name} {pk_field_type} PRIMARY KEY NOT NULL,
        {value_field_name} {value_field_type} NOT NULL
    )
    '''
    
    
    def __init__(self, schema_mapping_context):
        self._context = schema_mapping_context
        self._schema = 'test'
        

    def generate_fact_table_ddl(self):
        field_defs = []
        for field_name in self._context.dimension_names:
            field_def = '%s %s NOT NULL' % (field_name, self._context.sqltype_for_field(field_name))
            field_defs.append(field)

        for field_name in self._context.non_dimension_names:
            field_def = '%s %s NOT NULL' % (field_name, self._context.sqltype_for_field(field_name))
            
        field_def_block = ',\n'.join(field_defs)
        sql = fact_ddl_template.format(schema=self._schema,
                                      fact_table_name=self._context.fact.table_name,
                                      pk_field_name=self._context.fact.primary_key_field_name,
                                      fields=field_def_block)
        return sql


    def generate_dimension_table_ddl(self, dimension_name):
        dim = self._context.get_dimension(dimension_name)
        
        sql = dimension_ddl_template.format(schema=self._schema,
                                            pk_field_name=dim.primary_key_field_name,
                                            pk_field_type=self.sqltype(dim.primary_key_field_type),
                                            value_field_name=dim.value_field_name,
                                            value_field_type=self.sqltype(dim.value_field_type))
        return sql
        


class OLAPStarSchemaRelay(DataRelay):
    def __init__(self, persistence_mgr, olap_schema_map_ctx, **kwargs):
        DataRelay.__init__(self, **kwargs)
        self._pmgr = persistence_mgr
        self._dbconnection = self._pmgr.database.engine.connect()
        self._schema_mapping_context = olap_schema_map_ctx

        fact_record_type_builder = sqlx.SQLDataTypeBuilder('FactRecord', self._schema_mapping_context.fact.table_name)
        fact_record_type_builder.add_primary_key_field(self._schema_mapping_context.fact.primary_key_field_name,
                                                       self._schema_mapping_context.fact.primary_key_field_type)

        for name in self._schema_mapping_context.dimension_names:
            fact_record_type_builder.add_field(name,
                                               self._schema_mapping_context.get_dimension(name).fact_table_field_name,
                                               self._schema_mapping_context.get_dimension(name).primary_key_field_type)

        for name in self._schema_mapping_context.non_dimension_names:
            nd_field = self._schema_mapping_context.get_non_dimension_field(name)
            fact_record_type_builder.add_field(nd_field.field_name,
                                               nd_field.field_type)

        self._FactRecordType = fact_record_type_builder.build()


    def _handle_checkpoint_event(self, **kwargs):
        pass


    def _send(self, msg_header, kafka_message, **kwargs):
        log.debug("writing kafka log message to db...")
        log.debug('### kafka_message keys: %s' % '\n'.join(kafka_message.keys()))
        outbound_record = {}
        fact_data = self._schema_mapping_context.get_fact_values(kafka_message.get('body'), 
                                                                 persistence_manager=self._pmgr)

        print('### OLAP fact data:')
        print(common.jsonpretty(fact_data))

        insert_query_template = '''
        INSERT INTO {fact_table} ({field_names})
        VALUES ({data_placeholders});
        '''

        data_placeholder_segment = ', '.join([':%s' % name for name in fact_data.keys()])

        print('### initial rendering of insert statement: ')
        iqtemplate_render = insert_query_template.format(fact_table=self._schema_mapping_context.fact.table_name,
                                                         field_names=','.join(fact_data.keys()),
                                                         data_placeholders=data_placeholder_segment)
        print(iqtemplate_render)

        insert_statement = text(iqtemplate_render)
        insert_statement = insert_statement.bindparams(**fact_data)                                                     
        #dbconnection = self._pmgr.database.engine.connect()
        result = self._dbconnection.execute(insert_statement)



class ConsoleErrorHandler(object):
    def __init__(self):
        pass

    def handle_error(self, exception_obj):
        print('error in telegraf data operation:')
        print(str(exception_obj))



class SentryErrorHandler(object):
    def __init__(self, sentry_dsn):
        self.client = Client(sentry_dsn)


    def handle_error(self, exception_obj):
        self.client.send(str(exception_obj))



class IngestWritePromiseQueue(threading.Thread):
    '''Queues up the Future objects returned from KafkaProducer.send() calls
       and then handles the results of failed requests in a background thread
    '''

    def __init__(self, error_handler, futures = [], **kwargs):
        threading.Thread.__init__(self)
        self._futures = futures
        self._error_handler = error_handler
        self._errors = []
        self._sentry_logger = kwargs.get('sentry_logger')
        self._debug_mode = False
        if kwargs.get('debug_mode'):
            self._debug_mode = True

        self._future_retry_wait_time = 0.01


    def append(self, future):
        self._futures.append(future)
        return self
               
               
    def append_all(self, future_array):                
        self._futures.extend(future_array)
        return self
        


    def process_entry(self, f):
        result = {
            'status': 'ok',
            'message': ''
        }
        if not f.succeeded:
            result['status'] = 'error'
            result['message'] = f.exception.message
            log.error('write promise failed with exception: %s' % str(f.exception))
            self._sentry_logger.error('write promise failed with exception: %s' % str(f.exception))
            self._error_handler.handle_error(f.exception)
        return result


    def run(self):
        log.info('processing %d Futures...' % len(self._futures))
        results = []
        for f in self._futures:
            while not f.is_done:
                time.sleep(self._future_retry_wait_time)
            results.append(self.process_entry(f))
        self._errors = [r for r in results if r['status'] is not 'ok']
        log.info('all futures processed.')


    @property
    def size(self):
        return len(self._futures)


    @property
    def errors(self):
        return self._errors





