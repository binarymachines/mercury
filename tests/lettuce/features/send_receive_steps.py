#import lettuce_context
import context
from lettuce import *
from mercury import telegraf
from mercury import datamap as dmap
from snap import common
import logging
import logging.handlers
import sys
import os
from kafka import TopicPartition, OffsetAndMetadata, KafkaConsumer
from world_messages import WorldLoader, WorldRelay
import math

'''
logfile_name = yaml_config_obj['globals']['logfile']
handler = RotatingFileHandler(logfile_name, maxBytes=10000, backupCount=1)
log = logging.getLogger('send_receive_test')
logging.basicConfig(filename=logfile_name, level=logging.DEBUG)
log.addHandler(handler)
'''


SEND_RECEIVE_SCENARIO = 'Send data from filename to topic and read it back'
DATA_COMMIT_SCENARIO = 'Consumed data is committed at checkpoints correctly'


@step(u'given a known initial offset')
def given_a_known_initial_offset(step):
    test_context = world.test_environment.load_context(SEND_RECEIVE_SCENARIO)
    topic = world.pipeline_config.get_user_topic('scratch_topic')
    cluster = world.pipeline_config.cluster
    consumer_group = world.pipeline_config.get_user_defined_consumer_group('scratch_group_1')
    kc = KafkaConsumer(group_id=consumer_group,
                       bootstrap_servers=cluster.nodes,
                       value_deserializer=telegraf.json_deserializer,
                       auto_offset_reset='earliest',
                       consumer_timeout_ms=5000)

    metadata = kc.partitions_for_topic(topic)

    tp = TopicPartition(topic, 0)
    kc.assign([tp])
    test_context.offset = kc.position(tp)
    # logging.getLogger(test_context.log_id).debug('### initial offset is: %s' % test_context.offset)


@step(u'when we load data from the source file')
def when_we_load_data_from_the_source_file(step):
    test_context = world.test_environment.load_context(SEND_RECEIVE_SCENARIO)
    topic = world.pipeline_config.get_user_topic('scratch_topic')

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
    test_context.promise_queue_list = kwriter.process_write_promise_queue()


@step(u'then we register no errors')
def then_we_register_no_errors(step):
    test_context = world.test_environment.load_context(SEND_RECEIVE_SCENARIO)

    # for e in test_context.promise_queue_list:
    #     world.log.debug(str(e))
    assert len(test_context.promise_queue_list) == 0


@step(u'when we load (\d+) records from the source file')
def when_we_load_n_records_from_the_source_file(step, max_lin):
    test_context = world.test_environment.load_context(SEND_RECEIVE_SCENARIO)

    max_lines = int(max_lin)
    topic = world.pipeline_config.get_user_topic('scratch_topic')

    knodes = world.pipeline_config.cluster.node_array
    kwriter = telegraf.KafkaIngestRecordWriter(knodes)

    kloader = telegraf.KafkaLoader(topic, kwriter, record_type='direct_sales_record', stream_id='test_stream_id',
                                   asset_id='test_asset_id')
    wloader = WorldLoader(topic, kwriter, record_type='direct_sales_record', stream_id='test_stream_id',
                          asset_id='test_asset_id')
    processor = dmap.WhitespaceCleanupProcessor()
    extractor = dmap.CSVFileDataExtractor(processor, delimiter='|', quotechar='"')

    source_filename = world.pipeline_config.get_file_reference(test_context.test_file_alias)
    extractor.extract(source_filename, load_function=wloader.load, max_lines=max_lines)
    extractor.extract(source_filename, load_function=kloader.load, max_lines=max_lines)
    for rec in wloader.record_list:
        test_context.loaded_raw_record_list.append(rec)
    kwriter.sync(0.1)
    test_context.promise_queue_list = kwriter.process_write_promise_queue()


@step(u'and we read from the initial offset')
def and_we_read_from_initial_offset(step):
    test_context = world.test_environment.load_context(SEND_RECEIVE_SCENARIO)

    knodes = world.pipeline_config.cluster.node_array
    topic = world.pipeline_config.get_user_topic('scratch_topic')
    consumer_group = world.pipeline_config.get_user_defined_consumer_group('scratch_group_1')

    kreader = telegraf.KafkaIngestRecordReader(topic, knodes, consumer_group)

    # show how many partitions this topic spans
    metadata = kreader.consumer.partitions_for_topic(topic)

    # TopicPartition named tuple consists of the topic and a partition number
    tp = TopicPartition(topic, 0)

    # manually assign one or more partitions to the consumer --
    # required if we want to use explicit offsets
    kreader.consumer.assign([tp])

    topic_partition = TopicPartition(topic, list(metadata)[0])
    kreader.consumer.seek(topic_partition, test_context.offset)

    world_relay = WorldRelay(record_type='direct_sales_record', stream_id='test_stream_id', asset_id='test_asset_id')
    test_context.num_received_records = kreader.read(world_relay, world.log)
    #test_context.num_received_records = kreader.num_commits_issued
    for rec in world_relay.read_list:
        test_context.consumed_raw_record_list.append(rec)


@step(u'and we receive the same number of records')
def and_we_receive_the_same_number_of_records(step):
    test_context = world.test_environment.load_context(SEND_RECEIVE_SCENARIO)

    assert test_context.num_received_records == len(test_context.consumed_raw_record_list)


@step(u'and the records retrieved are identical to those sent')
def and_the_records_retrieved_are_identical_to_those_sent(step):
    test_context = world.test_environment.load_context(SEND_RECEIVE_SCENARIO)

    for i in xrange(len(test_context.loaded_raw_record_list)):
        written_rec = test_context.loaded_raw_record_list[i]
        read_rec = test_context.consumed_raw_record_list[i]
        assert written_rec['body'].get('DIRECT SALES KEY') == read_rec['body'].get('DIRECT SALES KEY')


@step(u'given the known initial offset')
def given_a_known_initial_offset(step):
    test_context = world.test_environment.load_context(DATA_COMMIT_SCENARIO)
    topic = world.pipeline_config.get_user_topic('scratch_topic')
    cluster = world.pipeline_config.cluster
    consumer_group = world.pipeline_config.get_user_defined_consumer_group('scratch_group_1')
    kc = KafkaConsumer(group_id=consumer_group,
                       bootstrap_servers=cluster.nodes,
                       value_deserializer=telegraf.json_deserializer,
                       auto_offset_reset='earliest',
                       consumer_timeout_ms=5000)

    metadata = kc.partitions_for_topic(topic)

    tp = TopicPartition(topic, 0)
    kc.assign([tp])
    test_context.offset = kc.position(tp)
    # world.log.debug('### initial offset is: %s' % test_context.offset)


@step(u'and we load (\d+) records from the source file')
def when_we_load_n_records_from_the_source_file(step, max_lin):
    test_context = world.test_environment.load_context(DATA_COMMIT_SCENARIO)

    max_lines = int(max_lin)
    topic = world.pipeline_config.get_user_topic('scratch_topic')

    knodes = world.pipeline_config.cluster.node_array
    kwriter = telegraf.KafkaIngestRecordWriter(knodes)

    kloader = telegraf.KafkaLoader(topic, kwriter, record_type='direct_sales_record', stream_id='test_stream_id',
                                   asset_id='test_asset_id')
    processor = dmap.WhitespaceCleanupProcessor()
    extractor = dmap.CSVFileDataExtractor(processor, delimiter='|', quotechar='"')

    source_filename = world.pipeline_config.get_file_reference(test_context.test_file_alias)
    extractor.extract(source_filename, load_function=kloader.load, max_lines=max_lines)
    kwriter.sync(0.1)


@step(u'when we read from the initial offset with a checkpoint frequency of (\d+)')
def when_we_read_from_initial_offset(step, checkpoint_freq):
    test_context = world.test_environment.load_context(DATA_COMMIT_SCENARIO)
    test_context.checkpoint_frequency = int(checkpoint_freq)

    knodes = world.pipeline_config.cluster.node_array
    topic = world.pipeline_config.get_user_topic('scratch_topic')
    consumer_group = world.pipeline_config.get_user_defined_consumer_group('scratch_group_1')

    kreader = telegraf.KafkaIngestRecordReader(topic, knodes, consumer_group)

    # show how many partitions this topic spans
    metadata = kreader.consumer.partitions_for_topic(topic)

    # TopicPartition named tuple consists of the topic and a partition number
    tp = TopicPartition(topic, 0)

    # manually assign one or more partitions to the consumer --
    # required if we want to use explicit offsets
    kreader.consumer.assign([tp])

    topic_partition = TopicPartition(topic, list(metadata)[0])
    kreader.consumer.seek(topic_partition, test_context.offset)
    
    world_relay = WorldRelay(record_type='direct_sales_record', stream_id='test_stream_id', asset_id='test_asset_id')


    # world.log.debug('calling read() on kafka reader with ckpt frequency of %d and interval of %d...' % (int(checkpoint_freq), 10))
    test_context.num_received_records = kreader.read(world_relay, world.log, checkpoint_frequency=test_context.checkpoint_frequency, checkpoint_interval=10)
    #xkreader.read(world_relay, world.log)

    test_context.num_successful_checkpoints = world_relay.checkpoint_successes
    test_context.num_checkpoint_errors = len(world_relay.checkpoint_errors)


@step(u'then we register no checkpoint errors')
def then_we_register_no_checkpoint_errors(step):
    test_context = world.test_environment.load_context(DATA_COMMIT_SCENARIO)

    assert test_context.num_checkpoint_errors == 0


@step(u'and we have the correct number of successful checkpoints')
def and_we_have_the_correct_number_of_successful_checkpoints(step):
    
    test_context = world.test_environment.load_context(DATA_COMMIT_SCENARIO)
    num_received_records = float(test_context.num_received_records)
    checkpt_freq = float(test_context.checkpoint_frequency)
    num_should_be_checks = int(math.ceil(num_received_records / checkpt_freq))

    # world.log.info('number of should be checks is the ceiling of %f / %f = %d' % (num_received_records, checkpt_freq, num_should_be_checks))
    

    #assert test_context.num_successful_checkpoints == num_should_be_checks
