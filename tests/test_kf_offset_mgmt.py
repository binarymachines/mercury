#!/usr/bin/env python

'''Usage: test_kf_offset_mgmt.py (--topic=<topic>) [--topic2=<topic>]


'''

import docopt
import logging
import datetime
from snap import common
from mercury import telegraf
from mercury import datamap
from kafka import TopicPartition, OffsetAndMetadata
from kafka import KafkaConsumer


def main(args):

    log = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    ch.setFormatter(formatter)
    log.setLevel(logging.DEBUG)
    log.addHandler(ch)

    knodes = []
    knodes.append(telegraf.KafkaNode('172.31.17.24'))
    knodes.append(telegraf.KafkaNode('172.31.18.160'))
    knodes.append(telegraf.KafkaNode('172.31.17.250'))
    cluster = telegraf.KafkaCluster().add_node(knodes[0]).add_node(knodes[1]).add_node(knodes[2])
    # a kafka group is a numbered context shared by some number of consumers
    #print 'target group is "%s", topic is "%s"' % (group, topic)
    #omc = telegraf.KafkaOffsetManagementContext(cluster, topic, consumer_group=group)

    '''
    NOTE: test harness is--
    spin up a consumer with topic T1 and group G1
    get partitions for topic T1
    spin up a consumer with topic T1 and group G2
    get partitions for topic T1
    
    are partition lists identical? Or is it adding partitions dynamically?
    '''

    group1 = 'test_group_1'
    group2 = 'test_group_2'

    topic1 = args['--topic']
    topic2 = args.get('--topic2')
    
    test_consumer_1 = KafkaConsumer(group_id=group1,
                                    bootstrap_servers=cluster.nodes,
                                    value_deserializer=telegraf.json_deserializer,
                                    auto_offset_reset='earliest',
                                    consumer_timeout_ms=5000)

    tc1_metadata = test_consumer_1.partitions_for_topic(topic1)

    test_consumer_2 = KafkaConsumer(group_id=group2,
                                    bootstrap_servers=cluster.nodes,
                                    value_deserializer=telegraf.json_deserializer,
                                    auto_offset_reset='earliest',
                                    consumer_timeout_ms=5000)

    tc2_metadata = test_consumer_2.partitions_for_topic(topic1)

    tc1_metadata_topic_2 = None
    if topic2:
        tc1_metadata_topic_2 = test_consumer_1.partitions_for_topic(topic2)

    if tc1_metadata:
        print '### partitions for topic %s:\n%s' % (topic1, '\n'.join([str(p) for p in tc1_metadata]))
    else:
        print 'No metadata returned for topic %s, group %s.' % (topic1, group1)
    
    if tc2_metadata:
        print '### partitions for topic %s:\n%s' % (topic1, '\n'.join([str(p) for p in tc2_metadata]))
    else:
        print 'No metadata returned for topic %s, group %s.' % (topic1, group2)

    if tc1_metadata_topic_2:
        print '### partitions for topic %s:\n%s' % (topic2, '\n'.join([str(p) for p in tc1_metadata_topic_2]))



    '''
    spin up a consumer with topic T2 and group G1
    manually assign a topic partition to the consumer
    write messages to the topic
    - how many partitions do we have?
    manually assign a second topic partition to the consumer
    write messages to the topic
    - update in number of partitions/groups?

    ___

    spin up two consumers, each in a different group (G1, G2), 
        pointed at the same topic partition
    write to that topic partition
    - do both consumers receive the message?



    '''
    '''
    # TopicPartition named tuple consists of the topic and a partition number
    tp = TopicPartition(topic, 0)

    # manually assign one or more partitions to the consumer --
    # required if we want to use explicit offsets
    kreader.consumer.assign([tp])
    offset = int(args['--from-offset'])

    log.info('offset: %d' % offset)
    kreader.consumer.seek(tp, offset)

    error_handler = telegraf.ConsoleErrorHandler()
    '''


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)


