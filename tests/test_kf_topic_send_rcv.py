#!/usr/bin/env python

'''Usage: kf_topic_rcv (--topic=<topic>) (--from-offset=<offset>) [max_messages]


'''

import context
import docopt
import logging
import datetime
from snap import common
from snap import telegraf as tg
from snap import datamap
from kafka import TopicPartition, OffsetAndMetadata


log = logging.getLogger(__name__)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s:%(message)s')
ch.setFormatter(formatter)
log.setLevel(logging.DEBUG)
log.addHandler(ch)

def generate_test_key(cb_record, **kwargs):
        return '%s_%s' % ('mx_test', datetime.datetime.now().isoformat())


class KafkaSendReceiveTest(unittest.TestCase):

    def setUp(self):
        knodes = []
        knodes.append(tg.KafkaNode('54.234.248.7'))
        knodes.append(tg.KafkaNode('52.203.214.24'))
        knodes.append(tg.KafkaNode('54.152.11.167'))

        self.kwriter = tg.KafkaIngestRecordWriter(knodes)
        self.target_topic = 'mercury_test_topic_%s' % '001' 
        

    def tearDown(self):
        pass

    def test_can_create_a_kafka_pipeline_config_from_yaml(self):
        self.fail()


    def test_loader_can_send_records(self):
        kloader = tg.KafkaLoader(self.target_topic, 
                                 self.kwriter,
                                 pipeline='mx_test_pipeline',
                                 record_type='mx_test_record')
        
        local_filename = args['--file']
        processor = dmap.WhitespaceCleanupProcessor()
        extractor = dmap.CSVFileDataExtractor(processor, delimiter='|', quotechar='"')

        extractor.extract(local_filename, load_function=self.kwriter.load)
        self.kwriter.sync(0.1)
        print len(self.kwriter.process_promise_write_queue())


    def test_reader_can_receive_sent_records(self):
        self.fail()
    


def main(args):
    

    

    # a kafka group is a numbered context shared by some number of consumers
    group = 'test_group_1'
    topic = args['--topic']

    print 'target group is "%s", topic is "%s"' % (group, topic)

    kreader = telegraf.KafkaIngestRecordReader(topic, knodes, group)

    #print dir(kreader.consumer)

    # show how many partitions this topic spans
    metadata = kreader.consumer.partitions_for_topic(topic)
    print '### partitions for topic %s:\n%s' % (topic, '\n'.join([str(p) for p in metadata]))

    

    # TopicPartition named tuple consists of the topic and a partition number
    tp = TopicPartition(topic, 0)

    # manually assign one or more partitions to the consumer --
    # required if we want to use explicit offsets
    kreader.consumer.assign([tp])

    offset = int(args['--from-offset'])

    log.info('offset: %d' % offset)
    kreader.consumer.seek(tp, offset)

    error_handler = telegraf.ConsoleErrorHandler()

    
    couchbase_relay = telegraf.CouchbaseRelay('172.30.0.1',
                                              'mx_data',
                                              'mx_test_rec',
                                              generate_test_key,
                                              transformer=data_transformer)
    '''
    #console_relay = telegraf.ConsoleRelay(transformer=data_transformer)
    console_relay = telegraf.ConsoleRelay()
    kreader.read(console_relay, log)
    #kreader.consumer.commit({ tp: OffsetAndMetadata(offset, None) })


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)


