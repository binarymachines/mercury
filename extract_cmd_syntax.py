#!/usr/bin/env python


'''
Usage:
    extract_cmd_syntax --dir <script_dir> --list <listfile>
'''


import os, sys
import json
import docopt

from snap import common

CUTOUTS = [
    'bqexport', 
    'bqexport-view', 
    'bqstream-dl', 
    'couchtalk', 
    'dfproc', 
    'j2df', 
    'nullbytefilter',
    's3-fgate',
    'rsexport',
    'rsexec',
    's3-gettbl',
    's3stream-dl',
    'snowexec'
]


def main(args):

    script_dir = args['<script_dir>']
    listfile = args['<listfile>']
    scriptfile_list = []

    with open(listfile, 'r') as f:
        for raw_line in f:
            line = raw_line.strip()
            if line:
                scriptfile_list.append(line)

    sys.path.append(os.path.join(os.getcwd(), 'tempdata'))
    sys.path.append(os.path.join(os.getcwd(), 'mercury'))


    for scriptfile in scriptfile_list:
        if scriptfile in CUTOUTS:
            continue
        else:
            script_path = os.path.join(script_dir, scriptfile)
            
            module = __import__(scriptfile)
            print(module.__doc__)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

