#!/usr/bin/env python


'''
Usage:
    extract_cmd_syntax --dir <script_dir> --list <listfile>
'''


import os, sys
import re
import json
import docopt

from snap import common

CUTOUTS = [
    'mdoc',
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

MDOC_RX = re.compile(r"\+mdoc\+")


MERCURY_BORDER = '______________________________________________'
MERCURY_BANNER = '+++  Mercury script: {script} +++'


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

    script_list = []
    doc_registry = []

    for scriptfile in scriptfile_list:
        if scriptfile in CUTOUTS:
            continue
        else:
            script_path = os.path.join(script_dir, scriptfile)
            
            module_text = None
            module = __import__(scriptfile)
            syntax = module.__doc__

            with open(script_path, 'r') as f:
                module_text = f.read()

            mdoc_tags = []
            for match in re.finditer(MDOC_RX, module_text):
                mdoc_tags.append(match)

            if len(mdoc_tags) == 2:
                start = mdoc_tags[0].span()[1]
                end = mdoc_tags[1].span()[0]

                help_text = module_text[start:end]
                script_name = scriptfile.split('.')[0]
                script_list.append(script_name)

                banner = MERCURY_BANNER.format(script=script_name)

                fulldoc = f'{MERCURY_BORDER}\n\n{banner}\n{MERCURY_BORDER}\n{syntax}{help_text}\n'
                #print(fulldoc)

                doc_registry.append(
                    {
                    'script_name': script_name,
                    'doc': fulldoc
                    }
                )
            else:
                print(f'WARNING: odd number of +mdoc+ tags detected in Mercury script {scriptfile.rstrip(".py")}. Emitting only usage string.',
                      file=sys.stderr)

    script_array_decl = str(',\n'.join(["'" + s + "'" for s in script_list]))
    

    print(json.dumps({
        'docspecs': doc_registry,
        'script_name_array': script_array_decl
    }))



if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

