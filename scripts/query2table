#!/usr/bin/env python

'''
Usage:
    query2table --schema <schema> --table <table> [--query <query>]
'''

import os, sys
from snap import common
import docopt


TABLE_CREATE_TEMPLATE = '''
CREATE TABLE `{schema}.{table_name}`
AS {query}
'''


def main(args):    
    input_query = ''
    if args['--query'] == True:
        input_query = args['<query>']        
    else:        
        for line in sys.stdin:
            if sys.hexversion < 0x03000000:
                line = line.decode('utf-8')
            input_query += line

    tbl_create_query = TABLE_CREATE_TEMPLATE.format(schema=args['<schema>'],
                                                    table_name=args['<table>'],
                                                    query=input_query)
    
    print(tbl_create_query)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)                 