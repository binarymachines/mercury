#!/usr/bin/env python

'''Usage: kf_topic_send (--file=<filename>) (--topic=<topic>) 


'''

import docopt
import logging
import csv
from snap import datamap as dmap
from snap import common
from snap import telegraf
from prx_transforms import CSVToDictionaryConverter
from prx_services import S3ServiceObject



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
    klog = telegraf.KafkaIngestRecordWriter(knodes)

    topic = args['--topic']
    kloader = KafkaLoader(topic, klog)
    '''
    s3_so = S3ServiceObject(log, local_temp_path='/tmp')
    local_filename = s3_so.download_object('labs.data.praxis', 'test/baxalta_indirectsales_test_20170517.txt')
    '''

    local_filename = args['--file']

    #processor = dmap.ConsoleProcessor(dmap.WhitespaceCleanupProcessor())
    processor = dmap.WhitespaceCleanupProcessor()
    extractor = dmap.CSVFileDataExtractor(processor, delimiter='|', quotechar='"')

    extractor.extract(local_filename, load_function=kloader.load)
    klog.sync(0.1)

    '''
    with open(local_filename, 'rb') as datafile:
        csv_reader = csv.DictReader(datafile, delimiter='|', quotechar='"')

        rowcount = 0
        for row in csv_reader:
            if not rowcount:
                header = row
                print '### header ###'
                print '\n'.join(header.keys())
            else:
                print common.jsonpretty(row)
            rowcount += 1
        
   
    error_handler = telegraf.ConsoleErrorHandler()
    pq = telegraf.IngestWritePromiseQueue(error_handler, log, debug_mode=True)

    header = telegraf.IngestRecordHeader('indirect_sales_record', 'test_stream_id', 'test_asset_id')
    topic = args['--topic']
    for msg_num in range(1, 5):
        msg_builder = telegraf.IngestRecordBuilder(header)
        msg_builder.add_field('greeting', 'hello Kafka')

        ingest_msg = msg_builder.build()
        future_write = klog.write(topic, ingest_msg)
        pq = pq.append(future_write)

    pq.start()
    klog.sync(1)
    '''

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)


