#!/usr/bin/env python

'''
Usage:        test_kf_pipeline_config <configfile>

'''

import docopt
import re
import yaml
import csv
from snap import common
from mercury import telegraf as tg
from mercury import mx_utils as mx


def main(args):

    config_filename = args['<configfile>']

    pipeline_cfg = None
    with open(config_filename, 'r') as f:
        config = yaml.load(f)
        pipeline_cfg = tg.KafkaPipelineConfig(config)


    print '### -------- Data pipeline config in file %s' % config_filename
    print 'kafka nodes: %s' % (pipeline_cfg.node_addresses)
    print 'staging topic: %s' % pipeline_cfg.staging_topic
    print 'raw topic: %s' % pipeline_cfg.raw_topic
    print 'user defined topics by alias:'
    for ut in pipeline_cfg._user_topics:
        print '> alias: %s, topic name: %s' % (ut, pipeline_cfg.get_user_topic(ut))

    for fileref in pipeline_cfg.file_ref_aliases:
        print '> file alias: %s, path: %s' % (fileref, pipeline_cfg.get_file_reference(fileref))

    for alias in pipeline_cfg.group_aliases:
        print '> group alias: %s, consumer group name: %s' % (alias, pipeline_cfg.get_user_defined_consumer_group(alias))


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
