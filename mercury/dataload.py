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



class ChannelWriteLogicNotFound(Exception):
    def __init__(self, *function_names):
        Exception.__init__(self, 'DataStore subclass is missing the channel-write functions: %s' % (', '.join(function_names)))


class NoSuchDatastore(Exception):
    def __init__(self, datastore_name):
        Exception.__init__(self, 'No datastore registered as "%s" in config file.' 
                           % datastore_name)


class DataStore(object):
    def __init__(self, service_object_registry, *channels, **kwargs):
        self.service_object_registry = service_object_registry
        self.channel_write_functions = {}
        self.channel_mode = False
        self.write_channels = []
        for c in channels:
            self.write_channels.append(c)
        
        # NOTE: may deprecate this
        self._selector_func = kwargs.get('channel_select_function')

        if len(self.write_channels):
            self.channel_mode = True            
            for channel_name in channels:
                func_name = 'write_%s' % channel_name
                if hasattr(self, func_name):
                    self.channel_write_functions[channel_name] = getattr(self, func_name)
                

    @property
    def channels(self):
        return self.write_channels


    def write_default_channel(self, recordset, **kwargs):
        channel = kwargs.get('channel')
        print('!!! Invoking default channel write method for channel ID "%s". Override in DataStore subclass.' % channel, file=sys.stderr)


    def has_channel(self, channel_id):
        return channel_id in self.write_channels


    def get_channel_write_function(self, channel):
        return self.channel_write_functions.get(channel, self.write_default_channel)


    def write(self, recordset, **kwargs):
        '''write each record in <recordset> to the underlying storage medium.
        Implement in subclass.
        '''
        pass


class DataStoreRegistry(object):
    def __init__(self, datastore_dictionary):
        self.data = datastore_dictionary

    def lookup(self, datastore_name):
        if not self.data.get(datastore_name):
            raise NoSuchDatastore(datastore_name)
        return self.data[datastore_name]

    def has_datastore(self, datastore_name):
        return True if self.data.get(datastore_name) else False


class RecordBuffer(object):
    def __init__(self, datastore, **kwargs):        
        self.data = []
        self.checkpoint_mgr = None        
        self.datastore = datastore


    def writethrough(self, **kwargs):
        '''write the contents of the record buffer out to the underlying datastore.
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
        self.override_channel = kwargs.get('channel')


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
        kwargs.update(channel=self.override_channel)
        if self.writes_since_last_reset == self.interval:
            self.record_buffer.flush(**kwargs)
            self.reset()


    def __enter__(self):
        return self


    def __exit__(self, *exc):
        self.record_buffer.writethrough(channel=self.override_channel)
        return False


