#!/usr/bin/env python


import core
import json
import constants as const
import snap



def rec2dict(obj):
    return obj.__dict__


def default_func(input_data, service_objects):
    return core.TransformStatus(json.dumps({'message': 'Hello from tdx jobstat!'}))


def list_pipelines_func(input_data, service_objects):
    couchbase_svc = service_objects.lookup('couchbase')
    jmgr = couchbase_svc.journal_manager
    records = jmgr.retrieve_all(const.RECTYPE_PIPELINE)
    data = map(rec2dict, records)
    return core.TransformStatus(json.dumps(data))
    
    
def list_jobs_func(input_data, service_objects):
    couchbase_svc = service_objects.lookup('couchbase')
    jmgr = couchbase_svc.journal_manager
    records = jmgr.retrieve_all(const.RECTYPE_JOB)
    data = map(rec2dict, records)
    return core.TransformStatus(json.dumps(data))
    









def list_ops_func(input_data, service_objects):
    raise snap.TransformNotImplementedException('list_ops_func')


