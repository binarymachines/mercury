#!/usr/bin/env python

'''
Usage:  
    s3stream-dl --manifest_uri <s3_uri> --format=<fmt> --list
    s3stream-dl --manifest_uri <s3_uri> --format=<fmt> [--delimiter=<delimiter>]
    s3stream-dl [-p] --path <s3_uri> --pfx <prefix> --format <fmt> [--d=<delimiter>] --xcfg=<xfile_cfg> --xmap=<xfile_map> --ncfg=<ngst_cfg> --ntarget=<ngst_target> [--nchannel=<channel>]
    s3stream-dl [-p] --path <s3_uri> --pfx <prefix> --format=<fmt> --ncfg=<ngst_cfg> --ntarget=<ngst_target> [--nchannel=<channel>]

Options: 
    -p,--parallel     : stream bucket contents in parallel        
'''

import os, sys
import json
import multiprocessing as mp
from snap import common
import docopt
import sh
from sh import aws  # AWS CLI must already be installed
from sh import gsutil
from sh import xfile  # Mercury ETL toolkit must already be installed
from sh import ngst


class DATA_FORMAT(object):
    CSV = 'csv'    
    PARQUET = 'parquet'

class Mode():
    SERIAL = 'serial'
    PARALLEL = 'parallel'


def list_bucket_files_for_prefix(prefix, bucket_uri, directory, data_format):
    extension = data_format
    target_uri = os.path.join(bucket_uri, directory, '%s_*.%s' % (tablename, extension))
    filenames = [name.lstrip().rstrip() for name in gsutil.ls(target_uri)]
    return filenames


def get_bucket_filecount_for_table(tablename, bucket_uri, directory, data_format):
    return len(list_bucket_files_for_table(tablename, bucket_uri, directory, data_format))


def stream_file_contents_direct_to_ngst(file_uri,
                                        ngst_configfile,
                                        ngst_target,
                                        mode,
                                        channel=None):
    module = __name__
    parent = os.getppid()
    pid = os.getpid()

    if channel:
        ngst_cmd = ngst.bake('--config', ngst_configfile, '--target', ngst_target, '--channel=%s' % channel)
    else:
        ngst_cmd = ngst.bake('--config', ngst_configfile, '--target', ngst_target)



    for line in ngst_cmd(gsutil('cp', file_uri, '-', _piped=True), _iter=True):
        if mode == Mode.SERIAL:
            print(line, file=sys.stderr)
        else:
            print('[%s:%s (child_proc_%s)]: %s' % (module, parent, pid, line), file=sys.stderr)


def relay_file_contents_to_ngst(file_uri,
                                data_format,
                                delimiter,
                                xfile_configfile,
                                xfile_map,
                                ngst_configfile,
                                ngst_target,
                                mode,
                                channel=None):
    module = __name__
    parent = os.getppid()
    pid = os.getpid()

    if delimiter:
        xfile_cmd = xfile.bake('--config', xfile_configfile, '--delimiter', delimiter, '--map', xfile_map, '-s')
    elif data_format == DATA_FORMAT.JSON:
        xfile_cmd = xfile.bake('--config', xfile_configfile, '--json', '--map', xfile_map, '-s')
    else:
        raise Exception('only csv and json formats are currently supported.')

    if channel:
        ngst_cmd = ngst.bake('--config', ngst_configfile, '--target', ngst_target, '--channel=%s' % channel)
    else:
        ngst_cmd = ngst.bake('--config', ngst_configfile, '--target', ngst_target)

    for line in ngst_cmd(xfile_cmd(gsutil('cp', file_uri, '-', _piped=True), _piped=True), _iter=True):
        if mode == Mode.SERIAL:
            print(line, file=sys.stderr)
        else:
            print('[%s:%s (child_proc_%s)]: %s' % (module, parent, pid, line), file=sys.stderr)


def stream_file_contents(file_uri, xfile_configfile, delimiter, xfile_map, mode):
    module = __name__
    parent = os.getppid()
    pid = os.getpid()

    xfile_cmd = xfile.bake('--config', xfile_configfile, '--delimiter', delimiter, '--map', xfile_map, '-s')    

    for line in xfile_cmd(gsutil('cp', file_uri, '-', _piped=True), _iter=True):
        if mode == Mode.SERIAL:
            print(line, file=sys.stderr)
        else:
            print('[%s:%s (child_proc_%s)]: %s' % (module, parent, pid, line), file=sys.stderr)


def main(args):
    data_format = args['--format']
    if data_format == DATA_FORMAT.CSV:
        if args.get('--xcfg') is not None and args.get('--d') is None:
            print('### csv chosen as the data format, but no delimiter specified.', file=sys.stderr)
    elif data_format != DATA_FORMAT.JSON:
        print('!!! supported data formats are "csv" and "json".', file=sys.stderr)
        return

    tablename = args['<table>']
    bucket = args['<bucket>']
    directory = ''
    if args.get('--dir') is not None:
        directory = args['--dir']

    if args.get('--list'):
        files = list_bucket_files_for_table(tablename, bucket, directory, data_format)
        print('\n'.join(files))
        return

    parallel_mode = False
    if args['--parallel']:
        parallel_mode = True

    xfile_bypass_mode = False
    xfile_config = args.get('--xcfg')
    xfile_map = args.get('--xmap')
    if xfile_config is None and xfile_map is None:
        xfile_bypass_mode = True 
        print('### operating in xfile_bypass mode.', file=sys.stderr)

    ngst_config = args.get('--ncfg')
    ngst_target = args.get('--ntarget')
    delimiter = args.get('--d')  # if no delimiter is supplied, we will assume JSON data

    if parallel_mode:
        for file_uri in list_bucket_files_for_table(tablename, bucket, directory, data_format):
            channel_id = args.get('--nchannel')  # can be null
            if xfile_bypass_mode:
                try:                    
                    stream_args = (file_uri,
                                    ngst_config,
                                    ngst_target,
                                    Mode.PARALLEL,
                                    channel_id)
                    p = mp.Process(target=stream_file_contents_direct_to_ngst,
                                   args=stream_args)
                    p.start()
                    p.join()
                except sh.ErrorReturnCode as e:
                    print(e.stderr)
                except Exception as e:
                    print(e)
            else:
                try:                    
                    stream_args = (file_uri,
                                   data_format,        
                                   delimiter,
                                   xfile_config,
                                   xfile_map,
                                   ngst_config,
                                   ngst_target,
                                   Mode.PARALLEL,
                                   channel_id)
                    
                    p = mp.Process(target=relay_file_contents_to_ngst,
                                   args=stream_args)
                    p.start()
                    p.join()
                except sh.ErrorReturnCode as e:
                    print(e.stderr)
                except Exception as e:
                    print(e)

    else:
        for file_uri in list_bucket_files_for_table(tablename, bucket, directory, data_format):
            channel_id = args.get('--nchannel')
            try:
                if xfile_bypass_mode:
                    stream_file_contents_direct_to_ngst(file_uri,                                              
                                                        ngst_config,
                                                        ngst_target,
                                                        Mode.SERIAL,
                                                        channel_id)
                else:
                    relay_file_contents_to_ngst(file_uri,
                                                data_format,
                                                delimiter,
                                                xfile_config,                                            
                                                xfile_map,
                                                ngst_config,
                                                ngst_target,
                                                Mode.SERIAL,
                                                channel_id)
            except sh.ErrorReturnCode as e:
                print(e.stderr)                
            except Exception as e:
                print(e)

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)