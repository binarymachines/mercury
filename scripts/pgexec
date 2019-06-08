#!/usr/bin/env python

'''
Usage:
    pgexec [-p] --target <alias> --db <database> -q <query>
    pgexec [-p] --target <alias> --db <database> -f <sqlfile> [--params=<n:v>...]
    pgexec [-p] --target <alias> --db <database> -s
    pgexec --targets

Options:
    -f --file       execute contents of SQL file
    -q --query
    -s --stdin      execute SQL passed to standard input
    -p --preview    show (but do not execute) SQL command(s)
'''

import os, sys
import json
from pathlib import Path
from collections import namedtuple
import sh
from sh import psql
import docopt
from snap import common


CONFIGFILE_NAME = 'config.yaml'
CONFIG_INSTRUCTIONS = '''
pgexec requires a YAML configuration file named "config.yaml" either in the directory
~/.pgx, or in the location specified by the environment variable PGX_CFG. 
'''

class ConfigDirNotFound(Exception):
    def __init__(self):
        pass

class NoSuchAlias(Exception):
    def __init__(self, alias):
        super().__init__(self, 'No alias registered under the name "%s".' % alias)
        self.alias = alias

TargetConfig = namedtuple('TargetConfig', 'host port user password')

def load_targets():
    targets = {}
    config_dir = os.getenv('PGX_CFG') or os.path.join(str(Path.home()), '.pgx')
    if not os.path.isdir(config_dir):
        raise ConfigDirNotFound()
    
    configfile = os.path.join(config_dir, 'config.yaml')
    yaml_config = common.read_config_file(configfile)
    
    for name in yaml_config['targets']:            
        hostname = yaml_config['targets'][name]['host']
        port = int(yaml_config['targets'][name].get('port') or '5432')
        user = yaml_config['targets'][name]['user']
        password = None
        if yaml_config['targets'][name].get('password'):
            password = common.load_config_var(yaml_config['targets'][name]['password'])
        targets[name] = TargetConfig(host=hostname, port=port, user=user, password=password)
    return targets


def read_stdin():
    for line in sys.stdin:
        if sys.hexversion < 0x03000000:
            line = line.decode('utf-8')
        yield line.lstrip().rstrip()


def main(args):    
    preview_mode = False
    if args['--preview']:
        preview_mode = True

    targets = load_targets()   
    if args['--targets']:
        for alias, raw_target in targets.items():
            target = raw_target._asdict()
            if target.get('password'):
                target['password'] = '*************'

            record = {
                alias: target
            }
            print(json.dumps(record))
        return

    target_alias = args['<alias>']
    if not targets.get(target_alias):
        raise NoSuchAlias(target_alias)

    try:
        psql_args = ['-w',
                     '-h', targets[target_alias].host,
                     '-p', targets[target_alias].port,
                     '-U', targets[target_alias].user,
                     '-d', args['<database>']]
        
        if args['--file'] is True:
            psql_args.extend(['-f', args['<sqlfile>']])

        elif args['--query'] is True:
            psql_args.extend(['-c', args['<query>']])

        else:
            query = ''
            for line in read_stdin():
                query = query + line
            psql_args.extend(['-c', query])

        psql_cmd = psql.bake(*psql_args)
        output = psql_cmd()
        
        if not output.exit_code:
            print(output.stdout.decode())
        else:
            print(output.stderr.decode())

    except ConfigDirNotFound:
        print(CONFIG_INSTRUCTIONS)
        return
    except NoSuchAlias as err:
        print(str(err))
        return 


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)