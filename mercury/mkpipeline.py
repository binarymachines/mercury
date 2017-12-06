#!/usr/bin/env python

'''Usage: mkpipeline.py <init_file> <pipeline_name>

'''


import docopt
import os
import logging
import constants as const
import datetime
import tdxutils as tdx
import couchbase
import snap
from snap import common
import couchbasedbx as cbx




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
    pipeline_name = args['<pipeline_name>']

    
    yaml_config = common.read_config_file(init_filename)
    logger = tdx.init_logging('mkpipeline', 'jobstat.log', logging.DEBUG)
    service_objects = snap.initialize_services(yaml_config, logger)
    so_registry = common.ServiceObjectRegistry(service_objects)
    couchbase_svc = so_registry.lookup('couchbase')
    jrnl_mgr = couchbase_svc.journal_manager
    
    jrnl_mgr.register_keygen_function(const.RECTYPE_PIPELINE, generate_pipeline_key)
    new_pipeline = PipelineRecord(pipeline_name, 'for populating orders star schema')

    try:
        key = jrnl_mgr.insert_record(new_pipeline)
        print('created pipeline "%s".' % new_pipeline.name)
        exit(0)
    except couchbase.exceptions.KeyExistsError, err:
        print('error: there is already a pipeline named "%s".' % pipeline_name)
        exit(1)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
    
