#!/usr/bin/env python 


# -*- coding: utf-8 -*-
import context
#import lettuce_context
from lettuce import step
from lettuce import world
from mercury import telegraf, sqldbx
from mercury import datamap as dmap
from snap import common
import os, docopt, logging, sys
from kafka import TopicPartition, KafkaConsumer
from world_messages import WorldLoader, WorldRelay

from sqlalchemy import Integer, String, DateTime, Float, Boolean, text, and_, bindparam
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

log = logging.getLogger(__name__)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s:%(message)s')
ch.setFormatter(formatter)
log.setLevel(logging.DEBUG)
log.addHandler(ch)

EXTRACT_TRANSFORM_CONSUME_SCENARIO = 'etl_raw_records_to_olap'


@step(u'given we load data from the source file to the source topic')
def given_we_load_data_from_the_source_file_to_the_source_topic(step):
    topic = world.pipeline_config.raw_topic
    test_context = world.test_environment.load_context(EXTRACT_TRANSFORM_CONSUME_SCENARIO)

    knodes = world.pipeline_config.cluster.node_array
    kwriter = telegraf.KafkaIngestRecordWriter(knodes)

    kloader = telegraf.KafkaLoader(topic, kwriter, record_type='direct_sales_record', stream_id='test_stream_id',
                                   asset_id='test_asset_id')
    wloader = WorldLoader(topic, kwriter, record_type='direct_sales_record', stream_id='test_stream_id',
                          asset_id='test_asset_id')
    processor = dmap.WhitespaceCleanupProcessor()
    extractor = dmap.CSVFileDataExtractor(processor, delimiter='|', quotechar='"')
    source_filename = world.pipeline_config.get_file_reference(test_context.test_file_alias)
    extractor.extract(source_filename, load_function=wloader.load)
    extractor.extract(source_filename, load_function=kloader.load)
    kwriter.sync(0.1)
    for rec in wloader.record_list:
        test_context.loaded_raw_record_list.append(rec)
    test_context.promise_queue_errors = kwriter.process_write_promise_queue()


def get_offset(topic):
    cluster = world.pipeline_config.cluster
    consumer_group = world.pipeline_config.get_user_defined_consumer_group('scratch_group_2')

    kc = KafkaConsumer(group_id=consumer_group,
                       bootstrap_servers=cluster.nodes,
                       value_deserializer=telegraf.json_deserializer,
                       auto_offset_reset='earliest',
                       consumer_timeout_ms=5000)

    metadata = kc.partitions_for_topic(topic)

    tp = TopicPartition(topic, list(metadata)[0])
    kc.assign([tp])
    offset = kc.position(tp)
    return offset


@step(u'when we read and transform the records')
def when_we_read_and_transform_the_records(step):
    test_context = world.test_environment.load_context(EXTRACT_TRANSFORM_CONSUME_SCENARIO)

    mssql_db = sqldbx.SQLServerDatabase('', 'Legacy')

    db_username = world.mssql_username
    db_password = world.mssql_password

    mssql_db.login(db_username, db_password, schema='mercury')
    pmgr = sqldbx.PersistenceManager(mssql_db)

    transform_map_filename = world.pipeline_config.transform_map
    map_file_path = os.path.join(world.data_dir, transform_map_filename)

    transformer_builder = dmap.RecordTransformerBuilder(map_file_path,
                                                        persistence_mgr=pmgr)

    tfmr = transformer_builder.build()

    knodes = world.pipeline_config.cluster.node_array

    # a kafka group is a numbered context shared by some number of consumers
    group = world.pipeline_config.get_user_defined_consumer_group('scratch_group_2')
    topic = world.pipeline_config.raw_topic
    kreader = telegraf.KafkaIngestRecordReader(topic, knodes, group)

    # show how many partitions this topic spans
    metadata = kreader.consumer.partitions_for_topic(topic)
    print '### partitions for topic %s:\n%s' % (topic, '\n'.join([str(p) for p in metadata]))

    # TopicPartition named tuple consists of the topic and a partition number
    tp = TopicPartition(topic, 0)

    # manually assign one or more partitions to the consumer --
    # required if we want to use explicit offsets
    kreader.consumer.assign([tp])

    offset = get_offset(topic)
    topic_partition = TopicPartition(topic, list(metadata)[0])
    kreader.consumer.seek(topic_partition, offset)

    world_relay = WorldRelay(transformer=tfmr)
    kreader.read(world_relay, log)

    for rec in world_relay.read_list:
        test_context.consumed_raw_into_sst_record_list.append(rec)


@step(u'and load the core records')
def and_load_the_core_records(step):
    test_context = world.test_environment.load_context(EXTRACT_TRANSFORM_CONSUME_SCENARIO)
    knodes = world.pipeline_config.cluster.node_array
    kwriter = telegraf.KafkaIngestRecordWriter(knodes)
    topic = world.pipeline_config.staging_topic
    kloader = telegraf.KafkaLoader(world.pipeline_config.staging_topic, kwriter, record_type='direct_sales_record',
                                   stream_id='test_stream_id',
                                   asset_id='test_asset_id')
    wloader = WorldLoader(world.pipeline_config.staging_topic, kwriter, record_type='direct_sales_record',
                          stream_id='test_stream_id',
                          asset_id='test_asset_id')
    # print "\n\n #### record header = %s, record body = %s" % (
    # test_context.consumed_raw_into_sst_record_list[0]['header'],
    # test_context.consumed_raw_into_sst_record_list[0]['body'])

    for record in test_context.consumed_raw_into_core_record_list:
        wloader.load(record['body'])
        kloader.load(record['body'])

    kwriter.sync(0.1)
    for rec in wloader.record_list:
        test_context.loaded_sst_record_list.append(rec)


@step(u'and we consume the core records from staging into OLAP')
def and_we_consume_the_sst_records_from_staging_into_olap(step):
    test_context = world.test_environment.load_context(EXTRACT_TRANSFORM_CONSUME_SCENARIO)

    mapping_schema_fn = world.mapping_schema_filename
    context_builder = telegraf.OLAPSchemaMappingContextBuilder(mapping_schema_fn, context_name='core')
    mapping_context = context_builder.build()

    db = sqldbx.PostgreSQLDatabase(world.pgsql_host, 'mercury')
    db.login(world.pgsql_username, world.pgsql_password)
    pmgr = sqldbx.PersistenceManager(db)

    oss_relay = telegraf.OLAPStarSchemaRelay(pmgr, mapping_context)
    world_relay = WorldRelay()

    group = world.pipeline_config.get_user_defined_consumer_group('scratch_group_2')
    topic = world.pipeline_config.staging_topic
    kreader = telegraf.KafkaIngestRecordReader(topic, world.pipeline_config.cluster.node_array, group)

    # show how many partitions this topic spans
    metadata = kreader.consumer.partitions_for_topic(topic)
    print '### partitions for topic %s:\n%s' % (topic, '\n'.join([str(p) for p in metadata]))

    tp = TopicPartition(topic, 0)
    kreader.consumer.assign([tp])

    offset = get_offset(topic)
    topic_partition = TopicPartition(topic, list(metadata)[0])

    kreader.consumer.seek(topic_partition, offset)

    kreader.read(oss_relay, log)
    kreader.consumer.seek(topic_partition, offset)
    kreader.read(world_relay, log)
    for rec in world_relay.read_list:
        test_context.consumed_sst_into_olap_record_list.append(rec)


@step(u'then we get the same number of core records in the target topic')
def then_we_get_the_same_number_of_sst_records_in_the_target_topic(step):
    test_context = world.test_environment.load_context(EXTRACT_TRANSFORM_CONSUME_SCENARIO)
    assert len(test_context.loaded_raw_record_list) == len(test_context.consumed_raw_into_sst_record_list)
    assert len(test_context.loaded_raw_record_list) == len(test_context.loaded_sst_record_list)


@step(u'and we get the same number of records in the fact table of the schema')
def and_we_get_the_same_number_of_records_in_the_fact_table_of_the_schema(step):
    test_context = world.test_environment.load_context(EXTRACT_TRANSFORM_CONSUME_SCENARIO)
    assert len(test_context.loaded_raw_record_list) == len(test_context.consumed_core_into_olap_record_list)
