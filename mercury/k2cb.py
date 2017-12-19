#!/usr/bin/env python



from telegraf import DataRelay
import couchbasedbx as cbx
import logging

log = logging.getLogger(__name__)


class CouchbaseRelay(DataRelay):
    def __init__(self, host, bucket, record_type, keygen_function, **kwargs):
        DataRelay.__init__(self, **kwargs)
        self._record_type = record_type
        couchbase_server = cbx.CouchbaseServer(host)
        self._couchbase_mgr = cbx.CouchbasePersistenceManager(couchbase_server, bucket)
        self._couchbase_mgr.register_keygen_function(self._record_type, keygen_function)


    def _send(self, src_message_header, message_data):
        builder = cbx.CouchbaseRecordBuilder(self._record_type)
        builder.from_json(message_data)
        cb_record = builder.build()
        key = self._couchbase_mgr.insert_record(cb_record)
        log.info('new record key: %s' % key)


