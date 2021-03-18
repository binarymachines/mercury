#!/usr/bin/env python

import sys
import datetime
from collections import namedtuple
from abc import ABC, abstractmethod
from snap import common
from mercury import datamap as dmap


ProfileJobParameter = namedtuple('ProfileJobParameter', 'name datatype default')
ProfilingStatus = namedtuple('ProfilingStatus', 'data record_count')


class Profiler(ABC):
    def __init__(self, *fields, **kwargs):
        self.parameters = []
        self.null_value_equivalents = {}


    def add_job_parameter(self, param: ProfileJobParameter):
        self.parameters.append(param)
    

    def map_column_values_to_null(self, column_name, *values):
        '''inform the ProfileDataset that the passed value, 
        if it appears in the named column, should be considered
        the same as a NULL value'''

        # TODO: implement this 
        pass

    
    @abstractmethod
    def _profile(self, record_generator, service_registry, **kwargs) -> ProfilingStatus:
        '''perform data profiling on records from record_generator.
        Implement in subclass.'''
        pass


    def verify_param_type(self, param_name, job_param: ProfileJobParameter) -> bool:
        '''TODO: check datatype of incoming parameter '''
        return True


    def profile(self, record_generator, service_registry, **kwargs) -> dict:
        status = {}
        errors = []
        job_parameters = kwargs

        for param in self.parameters:

            print(f'### Checking for required parameter {param.name}...', file=sys.stderr)
            if kwargs.get(param.name) is None:
                if param.default is None:
                    errors.append(f'Missing required job parameter "{param.name}" (no default provided)')
                else:
                    job_parameters[param.name] = param.default

            if kwargs.get(param.name) is not None:
                if not self.verify_param_type(kwargs[param.name], param):
                    errors.append(f'Parameter type mismatch: parameter {param.name} must be of type {param.datatype}')

        if len(errors):
            status['ok'] = False
            status['timestamp'] = datetime.datetime.now().isoformat()
            status['errors'] = errors
            return status
        
        profile_status = self._profile(record_generator, service_registry, **job_parameters)
        status['ok'] = True
        status['timestamp'] = datetime.datetime.now().isoformat()
        status['parameter_values'] = job_parameters
        status['record_count'] = profile_status.record_count
        status['data'] = profile_status.data
        
        return status


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
    def create(cls, job_name, yaml_config, **kwargs):        
        profiler_classes = cls.load_profiler_classes(yaml_config)
        job_config = yaml_config['jobs'].get(job_name)
        if not job_config:
            raise Exception('No data profiling job "%s" registered in the config file.' % job_name)
        
        profiler_alias = job_config['profiler']
        profiler_class = profiler_classes.get(profiler_alias)
        if not profiler_class:
            raise Exception(f'No data profiler class registered under the alias "{profiler_alias}".')

        profiler = profiler_class()
        profile_job_params = {}

        for parameter in job_config.get('parameters', {}):
            name = parameter['name']
            datatype = parameter['datatype']
            default_value = parameter.get('default')
            jobparam = ProfileJobParameter(name, datatype, default_value)
            profiler.add_job_parameter(jobparam)

        if job_config.get('null_equivalents'):
            for fieldname, value_array in job_config['null_equivalents'].items():
                profiler.map_field_values_to_null(fieldname, *value_array)

        return profiler