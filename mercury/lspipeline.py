#!/usr/bin/env python

'''Usage: lspipeline.py <init_file>

'''


import docopt
import os
import logging
import constants as const
import datetime
import tdxutils as tdx
import couchbase
from snap import snap
from snap import couchbasedbx as cbx
from snap import common



class PipelineRecord(cbx.CouchbaseRecord):
    def __init__(self, pipeline_name, description):
        cbx.CouchbaseRecord.__init__(self, const.RECTYPE_PIPELINE)
        self.name = pipeline_name
        self.description = description
        self.date_created = datetime.datetime.now().isoformat()

        
def generate_pipeline_key(pipeline_record):
    return '%s_%s' % (const.RECTYPE_PIPELINE, pipeline_record.name)
        


def main(args):
    init_filename = args['<init_file>']
    
    yaml_config = common.read_config_file(init_filename)
    logger = tdx.init_logging('mkpipeline', 'jobstat.log', logging.DEBUG)
    service_objects = snap.initialize_services(yaml_config, logger)
    so_registry = common.ServiceObjectRegistry(service_objects)
    couchbase_svc = so_registry.lookup('couchbase')
    jrnl_mgr = couchbase_svc.journal_manager

    query = 'select * from tdx_journal where record_type = "%s"' % const.RECTYPE_PIPELINE

    recnum = 1
    for record in jrnl_mgr.bucket.n1ql_query(query):
        print '[%d] %s (%s), created %s' % (recnum, record[jrnl_mgr.bucket_name]['name'], record[jrnl_mgr.bucket_name]['description'], record[jrnl_mgr.bucket_name]['date_created'])
        recnum += 1



if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
    
