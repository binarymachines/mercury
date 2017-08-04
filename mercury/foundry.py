#/usr/bin/env python


import yaml
import os
import sqlalchemy as sqla
import sqlalchemy_utils


class FieldSpec(object):
    def __init__(self, field_name, sql_type_name, **kwargs):
        self._name = field_name
        self._sqltype = sql_type_name

    @property
    def name(self):
        return self._name

    @property
    def sqltype(self):
        return self._sqltype



class TableSpec(object):
    def __init__(self, name, **kwargs):
        self._table_name = name
        self._primary_key_field = primary_key_field
        self._primary_key_type = primary_key_type
        self._data_fields = []
        self._meta_fields = []


    def add_data_field(self, field_spec):
        self._data_fields.append(field_spec)


    def add_meta_field(self, field_spec):
        self._meta_fields.append(field_spec)



class TableSpecBuilder(object):
    def __init__(self, table_name, **kwargs):
        self._name = table_name
        self._extra_params = {}
        for key, value in kwargs.iteritems():
            self._extra_params[key] = value

        self._tablespec = TableSpec(self._name)


    def add_data_field(self, f_name, f_type, **kwargs):
        self._tablespec.add_data_field(FieldSpec(f_name, f_type, **kwargs))


    def add_meta_field(self, f_name, f_type, **kwargs):
        self._tablespec.add_meta_field(FieldSpec(f_name, f_type, **kwargs))


    def build(self):
        return self._tablespec
    

class Accumulator(object):
    def __init__(self):
        pass


    def _load(self, loading_query, **kwargs):
        '''override in subclass'''
        raise Exception('The _load() method must be overridden in a subclass of Accumulator.')


    def clear(self):
        '''remove all staged data -- override in subclass'''


    def load_source_data(self, source_table_spec, reading_frame, max_generation=-1):
        current_gen = 0
        load_error = None
        while True:
            loading_query = self.generate_load_query(source_table_spec, reading_frame, current_gen)
            try:
                self._load(loading_query, **kwargs)
                if max_generation >= 0 and current_gen == max_generation:
                    break
                current_gen += 1 
            except Exception, err:
                self._clear()
                load_error = err
                break
        if load_error:
            raise load_error
                


    def backfill(self, source_table_spec, reading_frame, generation):
        pass


    def push(self, kafka_loader):
        '''override in subclass'''
        pass



class TimelimeExtractor(object):
    def __init__(self):
        pass


class TimeFrame(object):
    def __init__(self, start_date, end_date, **kwargs):
        pass







