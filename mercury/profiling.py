#!/usr/bin/env python

from snap import common
from mercury import journaling as jrnl
from mercury import datamap as dmap


class Profiler(object):
    def __init__(self, table_name, *columns, **kwargs):
        self.table_name = table_name
        self.columns = [c for c in columns]
        self.null_value_equivalents = {}
        for c in self.columns:
            self.null_value_equivalents[c] = set()


    def map_column_values_to_null(self, column_name, *values):
        '''inform the ProfileDataset that the passed value, 
        if it appears in the named column, should be considered
        the same as a NULL value'''

        if column_name not in self.null_value_equivalents.keys():
            raise Exception('no column "%s" registered with ProfileDataset< tablename: %s >.' % (column_name, self.table_name))
        self.null_value_equivalents[column_name].add(*values)


    def _profile(self, record_generator, service_registry, **kwargs):
        '''perform data profiling on records from record_generator.
        Implement in subclass.'''
        return ()


    def profile(self, record_generator, service_registry, **kwargs):
        time_log = dmap.TimeLog()
        result_tuple = None
        operation_name = kwargs.get('op_name') or 'profile dataset "%s"' % self.table_name
        with jrnl.stopwatch(operation_name, time_log):
            result_tuple = self._profile(record_generator, service_registry, **kwargs)

        print(common.jsonpretty(time_log.readout))
        return result_tuple


class ProfilerFactory(object):
    @classmethod
    def load_profiler_classes(cls, yaml_config):
        profiler_classes = {}
        profiler_module = yaml_config['globals']['profiler_module']
        project_home = common.load_config_var(yaml_config['globals']['project_home'])
        
        for profiler_alias in yaml_config['profilers']:
            profiler_config = yaml_config['profilers'][profiler_alias]
            profiler_module_name = yaml_config['globals']['profiler_module']
            classname = profiler_config['class']            
            klass = common.load_class(classname, profiler_module_name)
            profiler_classes[profiler_alias] = klass

        return profiler_classes


    @classmethod
    def create(cls, target_dataset, yaml_config, **kwargs):        
        profiler_classes = cls.load_profiler_classes(yaml_config)
        dataset_config = yaml_config['datasets'].get(target_dataset)
        if not dataset_config:
            raise Exception('!!! No dataset "%s" registered in config file.' % target_dataset)

        tablename = dataset_config['tablename']
        profiler_class = profiler_classes[dataset_config['profiler']]
        profiler = profiler_class(tablename, *dataset_config['columns'])
        if dataset_config.get('null_equivalents'):
            for colname, value_array in dataset_config['null_equivalents'].items():
                profiler.map_column_values_to_null(colname, *value_array)

        return profiler