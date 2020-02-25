#!/usr/bin/env python

from mercury import datamap as dmap
from mercury.dataload import DataStore, DataStoreRegistry, RecordBuffer, checkpoint


class TestDatastore(DataStore):
    def __init__(self, service_object_registry, *channels, **kwargs):
        super.__init__(service_object_registry, *channels, **kwargs)
        self.init_values = kwargs
        self.num_bulk_writes = 0
        self.num_record_writes = 0
        
    @property
    def init_param_fields(self):
        return [ key for key, value in self.init_values.items()]


    def _write(self, recordset, **kwargs):
        for record in recordset:
            self.num_record_writes += 1

        self.num_bulk_writes += 1
         