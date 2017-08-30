#!/usr/bin/env python



import context
import os, sys
import unittest
import docopt
import logging
import datetime
from snap import common
from mercury import telegraf as tg
from mercury import datamap as dmap
from kafka import TopicPartition, OffsetAndMetadata
from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner


LOG_ID = 'kafka_send_rcv_test'


logging.basicConfig( stream=sys.stderr)
log = logging.getLogger(LOG_ID)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s:%(message)s')
ch.setFormatter(formatter)
log.setLevel(logging.DEBUG)
log.addHandler(ch)


def generate_test_key(cb_record, **kwargs):
        return '%s_%s' % ('mx_test', datetime.datetime.now().isoformat())


class KafkaSendReceive(unittest.TestCase):
 
    def setUp(self):
        knodes = []
        knodes.append(tg.KafkaNode('172.30.0.164'))
        knodes.append(tg.KafkaNode('172.30.0.165'))
        knodes.append(tg.KafkaNode('172.30.0.166'))

        self.kwriter = tg.KafkaIngestRecordWriter(knodes)
        self.target_topic = 'mercury_test_topic_%s' % '001' 
        

    def tearDown(self):
        pass

    def test_can_create_a_kafka_pipeline_config_from_yaml(self):
        self.fail()


    def test_loader_can_send_records(self):
        kloader = tg.KafkaLoader(self.target_topic, 
                                 self.kwriter,
                                 pipeline_id='mx_test_pipeline',
                                 record_type='mx_test_record')
        
        local_filename = 'data/sample_objectstore.csv'
        #processor = dmap.WhitespaceCleanupProcessor()
        extractor = dmap.CSVFileDataExtractor(None, delimiter='|', quotechar='"')

        extractor.extract(local_filename, load_function=kloader.load)
        self.kwriter.sync(0.1)
        self.assertTrue(self.kwriter.promise_queue_size > 0)
        self.assertEquals(len(self.kwriter.process_write_promise_queue()), 0)


    def test_reader_can_receive_sent_records(self):
        self.fail()



if __name__ == '__main__':
    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
        
    unittest.main(testRunner=runner)

