''' Usage: test_olap_schema_map_builder.py (-m <map_name>) <configfile> 

'''

import context
import docopt
import os
import logging
import datetime
from snap import common
from mercury import telegraf as tg
from kafka import TopicPartition, OffsetAndMetadata
from sqlalchemy_utils import UUIDType
import prx_services as psvc



def main(args):

    print args

    yaml_file = args.get('<configfile>')
    context_id = args.get('<map_name>')

    print '### creating builder for schema mapping context defined in %s...' % yaml_file
    builder = tg.OLAPSchemaMappingContextBuilder(yaml_file, context_name=context_id)

    print '### building...'
    mapping_context = builder.build()

    print '### done.'


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

