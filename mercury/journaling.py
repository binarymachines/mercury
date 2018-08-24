#!/usr/bin/env python


from functools import wraps
import os
import datetime
import traceback



def generate_op_record_key(oplog_record):
    return '%s_%s' % (oplog_record.record_type, datetime.datetime.utcnow().isoformat()) 


class OpLogField(object):
    def __init__(self, name):
        self.name = name

    def _value(self):
        '''return data to be logged'''

    def data(self):
        return { self.name : self._value() }


class OpLogEntry(object):
    def __init__(self, **kwargs):
        self.fields = []


    def add_field(self, op_log_field):
        self.fields.append(op_log_field)
        return self


    def data(self):
        result = {}
        for field in self.fields:
            result.update(field.data())
        return result
    

class TimestampField(OpLogField):
    def __init__(self):
        OpLogField.__init__(self, 'timestamp')
        #self.time = datetime.datetime.now().isoformat() 


    def _value(self):
        return datetime.datetime.now().isoformat()

    
class StatusField(OpLogField):
    def __init__(self, status_name):
        OpLogField.__init__(self, 'status')
        self.status = status_name


    def _value(self):
        return self.status


class PIDField(OpLogField):
    def __init__(self):
        OpLogField.__init__(self, 'pid')
        self.process_id = os.getpid()


    def _value(self):
        return self.process_id

    
class RecordPageField(OpLogField):
    def __init__(self, reading_frame):
        OpLogField.__init__(self, 'record_page')
        self.reading_frame = reading_frame

    def _value(self):
        return { 'page_number': self.reading_frame.index_number,
                 'page_size': self.reading_frame.size
        }


class OpLogWriter(object):
    def write(self, **kwargs):
        '''implement in subclasses'''
        pass

    def update(self, key, **kwargs):
        '''optional: implement in subclass if we need dealing with a delta journal'''
        raise Exception('OpLogWriter.update() method not implemented in this class.')


class OpLogLoader(object):
    def load_oplog_entry(self, entry_key):
        '''implement in subclass'''
        pass

'''
class CouchbaseOpLogWriter(OpLogWriter):
    def __init__(self, record_type_name, couchbase_persistence_mgr, **kwargs):
        self.record_type_name = record_type_name
        self.pmgr = couchbase_persistence_mgr
        self.pmgr.register_keygen_function(self.record_type_name, generate_op_record_key)


    def write(self, **kwargs):
        op_record = CouchbaseRecordBuilder(self.record_type_name).add_fields(kwargs).build()
        return self.pmgr.insert_record(op_record)
'''


class ContextDecorator(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


    def __enter__(self):
        return self


    def __exit__(self, typ, val, traceback):
        pass


    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return wrapper



class journal(ContextDecorator):
    def __init__(self, op_name, oplog_writer, start_entry, end_entry = None):
        self.oplog_writer = oplog_writer
        self.op_name = op_name
        self.start_entry = start_entry
        self.end_entry  = end_entry
        

    def __enter__(self):
        # write the start record
        record = self.start_entry.data()
        record['op_name'] = self.op_name
        self.oplog_writer.write(**record)
        return self


    def __exit__(self, typ, val, exc_traceback):        
        #traceback.print_tb(exc_traceback)        
        if self.end_entry:
            # write end record, if we are doing double-ended journaling
            record = self.end_entry.data()
            record['op_name'] = self.op_name
            self.oplog_writer.write(**record)

        return self
    

class delta_journal(ContextDecorator):
    def __init_(self, op_name, oplog_writer, oplog_loader, oplog_entry, update_function):
        self.op_name = op_name
        self.oplog_writer = oplog_writer
        self.oplog_entry = oplog_entry
        self.oplog_entry_update_func = update_function
        self.oplog_entry_key = None


    def __enter__(self):
        record = self.oplog_entry.data()
        record.op_name = self.op_name
        print('writing oplog record. Original record is:\n%s' % record)
        self.oplog_entry_key = self.oplog_writer.write(**record)
        return self

    
    def __exit__(self, typ, val, traceback):
        record = self.oplog_loader.load(self.oplog_entry_key)
        updated_record = self.oplog_entry_update_func(record)
        self.oplog_writer.update(self.oplog_entry_key, **updated_record)


class TimeLog(object):
    def __init__(self):
        self.op_data = {}
        self.readout_data = {}


    def _elapsed_time_readout(self, start_time, end_time):
        diff = end_time - start_time
        days = diff.days # Get Day 
        hours, remainder = divmod(diff.seconds, 3600) # Get Hour 
        minutes, seconds = divmod(remainder, 60) # Get Minute & Second 

        return '%d day(s), %d hour(s), %d minute(s), and %d second(s)' % (days, hours, minutes, seconds)


    def record_elapsed_time(self, operation_tag, start_time, end_time):
        if self.op_data.get(operation_tag):
            raise Exception('attempted to overwrite the operation tag "%s" with new time data.' % operation_tag)
        self.op_data[operation_tag] = (start_time, end_time)


    @property
    def elapsed_time_data(self):
        result = {}
        for operation_tag, start_end_tuple in self.op_data.items():
            end_time = start_end_tuple[1]
            start_time = start_end_tuple[0]
            result[operation_tag] = end_time - start_time
        return result


    @property
    def data(self):
        return self.op_data


    @property
    def readout(self):
        result = {}
        for operation_tag, start_end_tuple in self.op_data.items():
            result[operation_tag] = self._elapsed_time_readout(start_end_tuple[0], start_end_tuple[1])
        return result


class stopwatch(ContextDecorator):
    def __init__(self, operation_tag, time_log):
        self.time_log = time_log
        self.tag = operation_tag
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = datetime.datetime.now()     
        return self

    def __exit__(self, typ, val, exc_traceback):        
        end_time = datetime.datetime.now()
        self.time_log.record_elapsed_time(self.tag, self.start_time, end_time)


class CountLog(object):
    def __init__(self):
        self.op_data = {}

    def _count_readout(self, operation_tag, count):
        return 'operation "%s" executed %d times.' % (operation_tag, count)

    def update_count(self, operation_tag, delta):
        if self.op_data.get(operation_tag) is None:
            self.op_data[operation_tag] = delta
        else:
            self.op_data[operation_tag] += delta

    @property
    def data(self):
        return self.op_data

    @property
    def readout(self):
        result = []
        for operation_tag, op_count in self.op_data.items():
            result.append(self._count_readout(operation_tag, op_count))
        return '\n'.join(result)


class counter(ContextDecorator):
    def __init__(self, operation_tag, count_log, count_delta=1):
        self.count_log = count_log
        self.tag = operation_tag
        self.count_delta = count_delta

    def __enter__(self):
        return self

    def __exit__(self, typ, val, exc_traceback):
        self.count_log.update_count(self.tag, self.count_delta)

'''
global_count_log = CountLog()
global_time_log = TimeLog()


@counter('call annotated function', global_count_log)
def some_func():
    pass


import time

@stopwatch('call timed function', global_time_log)
def timed_function():
    time.sleep(2)
'''  


