#!/usr/bin/env python


import os, sys
from contextlib import ContextDecorator
import csv
import json
import logging
from collections import namedtuple

import docopt
from docopt import docopt as docopt_func
from docopt import DocoptExit
from snap import snap, common
from mercury import datamap as dmap
import yaml


class DataStore(object):
    def __init__(self, service_object_registry, **kwargs):
        self.service_object_registry = service_object_registry


    def write(self, recordset, **kwargs):
        '''write each record in <recordset> to the underlying storage medium.
        Implement in subclass.
        '''
        pass


class RecordBuffer(object):
    def __init__(self, datastore, **kwargs):        
        self.data = []
        self.checkpoint_mgr = None        
        self.datastore = datastore


    def writethrough(self, **kwargs):
        '''write the contents of the record buffer out to the underlying datastore.
        Implement in subclass.
        '''
        self.datastore.write(self.data, **kwargs)


    def register_checkpoint(self, checkpoint_instance):
        self.checkpoint_mgr = checkpoint_instance


    def flush(self, **kwargs):        
        self.writethrough(**kwargs)
        self.data = []


    def write(self, record, **kwargs):
        try:
            self.data.append(record) 
            if self.checkpoint_mgr:
                self.checkpoint_mgr.register_write(**kwargs)          
        except Exception as err: 
            raise err


class checkpoint(ContextDecorator):
    def __init__(self, record_buffer, **kwargs):
        checkpoint_interval = int(kwargs.get('interval') or 1)

        self.interval = checkpoint_interval
        self._outstanding_writes = 0
        self._total_writes = 0
        self.record_buffer = record_buffer
        self.record_buffer.register_checkpoint(self)


    @property
    def total_writes(self):
        return self._total_writes

    @property
    def writes_since_last_reset(self):
        return self._outstanding_writes


    def increment_write_count(self):
        self._outstanding_writes += 1
        self._total_writes += 1


    def reset(self):
        self.outstanding_writes = 0


    def register_write(self, **kwargs):
        self.increment_write_count()
        if self.writes_since_last_reset == self.interval:
            self.record_buffer.flush(**kwargs)
            self.reset()


    def __enter__(self):
        return self


    def __exit__(self, *exc):
        self.record_buffer.writethrough()
        return False


