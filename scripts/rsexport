#!/usr/bin/env python

'''
Usage:  
    rsexport [-p] <db> <schema> --table <table> --bucket_uri <s3_uri> --delimiter <delimiter> [--iam <iam_role>]
    rsexport [-p] <db> <schema> --query <query> --bucket_uri <s3_uri> --delimiter <delimiter> [--iam <iam_role>] 
    rsexport [-p] <db> <schema> --query_template <template_file> --bucket <s3_uri> --delimiter <delimiter> 
    rsexport [-p] <db> <schema> --bucket_uri <s3_uri> [--iam <iam_role>] --delimiter <delimiter> [--directory=<directory>]

Options:
    -p --preview    run in preview mode; show SQL statement but do not execute
'''


import os, sys
import time
import docopt
from snap import common
import sqlalchemy as sqla
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.session import sessionmaker

from contextlib import contextmanager


UNLOAD_STATEMENT_TEMPLATE = """
UNLOAD ('{query}') 
TO
'{s3_uri}'
{auth_clause}
DELIMITER '{delimiter}' MANIFEST VERBOSE;
"""

def generate_credentials_auth_clause(**kwargs):
    kwreader = common.KeywordArgReader('access_key_id', 'access_key')
    kwreader.read(**kwargs)    
    return "credentials 'aws_access_key_id={access_key_id};aws_secret_access_key={access_key}'".format(**kwargs)


def generate_iam_auth_clause(**kwargs):
    kwreader = common.KeywordArgReader('iam_role_arn')
    kwreader.read(**kwargs)
    return "iam_role '{iam_role_arn}'".format(**kwargs)


@contextmanager
def connect_redshift(hostname, database, schema, username, password, port=5439):
    url_template = '{db_type}://{user}:{passwd}@{host}:{port}/{database}'
    db_url = url_template.format(db_type='redshift+psycopg2',
                                 user=username,
                                 passwd=password,
                                 host=hostname,
                                 port=5439,
                                 database=database)
    retries = 0
    connected = False
    while not connected and retries < 3:
        try:
            engine = sqla.create_engine(db_url, echo=False)
            metadata = MetaData(schema=schema)
            Base = automap_base(bind=engine, metadata=metadata)
            Base.prepare(engine, reflect=True)
            metadata.reflect(bind=engine)
            session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
            connected = True
            print('### Connected to Redshift DB.', file=sys.stderr)
            
        except Exception as err:
            print(err)
            print(err.__class__.__name__)
            print(err.__dict__)
            time.sleep(1)
            retries += 1
        
    if not connected:
        raise Exception('!!! Unable to connect to Redshift db on host %s at port %s.' % (hostname, 5439))


def generate_s3_uri(s3_bucket_name, directory=None):
    base_uri = None
    if not s3_bucket_name.startswith('s3://'):
        base_uri = 's3://' + s3_bucket_name
    else:
        base_uri = s3_bucket_name
    
    if directory:
        return os.path.join(base_uri, directory)

    return base_uri


def main(args):
    database = args['<db>']
    schema = args['<schema>']
    

    if args['--iam'] == True:
        auth_clause = generate_iam_auth_clause(iam_role_arn=args['<iam_role>'])
    else:
        key_id = os.getenv('AWS_SECRET_KEY_ID')
        key = os.getenv('AWS_SECRET_ACCESS_KEY')
        if not key_id or not key:
            print('the environment variables AWS_SECRET_KEY_ID and AWS_SECRET_ACCESS_KEY must be set.')
            return
        auth_clause = generate_credentials_auth_clause(access_key_id=key_id,
                                                       access_key=key)

    if args['--table'] == True:
        tablename = args['<table>']
        unload_query = 'SELECT * FROM %s.%s' % (schema, tablename)        
    elif args['--query'] == True:
        unload_query = args['<query>']
    else:
        # read from stdin
        unload_query = ''
        for line in sys.stdin:
            if sys.hexversion < 0x03000000:
                line = line.decode('utf-8')
            unload_query += line
        unload_query = unload_query.lstrip().rstrip()

    delimiter_char = args['<delimiter>']
    target_uri = generate_s3_uri(args['<s3_uri>'])
    unload_statement = UNLOAD_STATEMENT_TEMPLATE.format(query=unload_query,
                                                        s3_uri=target_uri,
                                                        auth_clause=auth_clause,
                                                        delimiter=delimiter_char)

    print(unload_statement)
        

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
