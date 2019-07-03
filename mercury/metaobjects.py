#!/usr/bin/env python

import json
from collections import namedtuple
from snap import common


class Parameter(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('name')
        kwreader.read(**kwargs)
        self.name = kwargs['name']
        self.value = kwargs.get('value')
    
    def data(self):
        return {'name': self.name, 'value': self.value }

class ServiceObjectSpec(object):
    def __init__(self, name, classname, **init_params):
        self.alias = name
        self.classname = classname
        self.init_params = []
        for key, value in init_params.items():
            self.init_params.append(Parameter(name=key, value=value))

    def add_init_param(self, name, value):
        self.init_params.append(Parameter(name=name, value=value))

    def add_paramspec(self, parameter):
        self.init_params.append(parameter)

    def data(self):
        return {
            'alias': self.alias,
            'class': self.classname,
            'init_params': [
                {'name': p.name, 'value': p.value} for p in self.init_params
            ]
        }

class XfileFieldSpec(object):
    def __init__(self, name, **params):
        self.name = name
        self.parameters = []
        for key, value in params.items():
            self.parameters.append(Parameter(name=key, value=value))

    def add_parameter(self, name, value):
        self.parameters.append(Parameter(name=name, value=value))

    def data(self):
        return {
            'name': self.name,
            'parameters': [ p.data() for p in self.parameters ]
        }


class XfileMapSpec(object):
    def __init__(self, name, lookup_source_name):
        self.name = name
        self.lookup_source = lookup_source_name
        self.fields = []
        self.settings = [
            Parameter(name='use_default_identity_transform', value=True)
        ]

    def add_field(self, name, **params):
        self.fields.append(XfileFieldSpec(name, **params))

    def add_field_spec(self, fieldspec):
        self.fields.append(fieldspec)

    def add_field_specs(self, fieldspec_array):
        self.fields.extend(fieldspec_array)

    def data(self):
        return {
            'name': self.name,
            'lookup_source': self.lookup_source,
            'fields': [f.data() for f in self.fields]
        }


class DatasourceSpec(object):
    def __init__(self, name, classname):
        self.name = name
        self.classname = classname

    def data(self):
        return {
            'name': self.name,
            'classname': self.classname
        }


class NgstDatastore(object):
    def __init__(self, name, classname):
        self.name = name
        self.classname = classname
        self.channel_selector_function = None
        self.channels = []
        self.init_params = []

    def add_init_param(self, name, value):
        self.init_params.append(Parameter(name=name, value=value))

    def add_paramspec(self, parameter):
        self.init_params.append(parameter)

    def data(self):
        return {
            'name': self.name,
            'init_params': [
                {'name': p.name, 'value': p.value} for p in self.init_params
            ]
        }
    
class NgstTarget(object):
    def __init__(self, name, datastore_name, checkpoint_interval):
        self.name = name
        self.datastore = datastore_name
        self.checkpoint_interval = checkpoint_interval

    def data(self):
        return {
            'name': self.name,
            'datastore': self.datastore,
            'checkpoint_interval': self.checkpoint_interval
        }

class QuasrJobIOSlot(object):
    def __init__(self, name, datatype):
        self.name = name
        self.datatype = datatype

    def data(self):
        return {
            'name': self.name,
            'datatype': self.datatype
        }

class QuasrTemplateSpec(object):
    def __init__(self, name, text):
        self.name = name
        self.text = text


class QuasrJobSpec(object):
    def __init__(self, name, template_alias):
        self.name = name
        self.template_alias = template_alias
        self.inputs = []
        self.outputs = []
        self.executor_function = None
        self.builder_function = None
        self.analyzer_function = None

    def add_input_slot(self, name, datatype):
        self.inputs.append(QuasrJobIOSlot(name, datatype))

    def add_output_slot(self, name, datatype):
        self.outputs.append(QuasrJobIOSlot(name, datatype))

    def data(self):
        return {
            'name': self.name,
            'template_alias': self.template_alias,
            'executor_function': self.executor_function,
            'builder_function': self.builder_function,
            'analyzer_function': self.analyzer_function,
            'inputs': [ i.data() for i in self.inputs ],
            'outputs': [ o.data() for o in self.outputs ]
        }