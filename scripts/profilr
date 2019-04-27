#!/usr/bin/env python

'''
Usage:
    profilr --config <configfile> --dataset <dataset_name> --format <format> [--datafile <file>] [--limit=<limit>]
    profilr --config <configfile> --list
'''

import os, sys
from snap import snap, common
from mercury import datamap as dmap
from mercury import journaling as jrnl
import profiling as prf
import docopt


class Format(object):
    CSV = 'csv'
    JSON = 'json'


def main(args):

    configfile = args['<configfile>']
    if args['--list'] is True:
        yaml_config = common.read_config_file(configfile)
        print('\n'.join([dataset for dataset in yaml_config['datasets']]))
        return

    datafile = None
    stream_input = True
    if args['--datafile'] is True:
        stream_input = False
        datafile = args['<file>']

    if args['<format>'] == 'csv':
        intake_format = Format.CSV
    elif args['<format>'] == 'json':
        intake_format = Format.JSON
    else:
        print('Intake format "%s" not supported.' % args['--format'])
        return

    rec_source = None
    limit = -1
    if args.get('--limit'):
        limit = int(args['--limit'])

    field_delimiter = '|'

    if stream_input: # read input from stdin
        if intake_format == Format.CSV:
            rec_source = dmap.RecordSource(dmap.csvstream_record_generator,
                                        delimiter=field_delimiter,
                                        limit=limit)
        else:
            rec_source = dmap.RecordSource(dmap.json_record_generator,
                                        limit=limit)
    else: # read input from file        
        if intake_format == Format.CSV:
            rec_source = dmap.RecordSource(dmap.csvstream_record_generator,
                                           filename=datafile,                           
                                           delimiter=field_delimiter,
                                           limit=limit)
        elif intake_format == Format.JSON: 
            rec_source = dmap.RecordSource(dmap.json_record_generator,
                                           filename=datafile,
                                           limit=limit)

    target_dataset = args['<dataset_name>']
    yaml_config = common.read_config_file(configfile)
    service_registry = snap.initialize_services(yaml_config)
    
    profiler = prf.ProfilerFactory.create(target_dataset, yaml_config)
    profile_dict, record_count = profiler.profile(rec_source.records(), service_registry)

    print(common.jsonpretty(profile_dict))    
    print('%d records processed.' % record_count)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)