#!/usr/bin/env python


from collections import namedtuple

InitParam = namedtuple('InitParam', 'name value')
Parameter = namedtuple('Parameter', 'name value')

class ServiceObjectSpec(object):
    def __init__(self, name, classname, **init_params):
        self.alias = name
        self.classname = classname
        self.init_params = []
        for key, value in init_params.items():
            self.init_params.append(InitParam(name=key, value=value))

    def add_init_param(self, name, value):
        self.init_params.append(InitParam(name=name, value=value))


class XfileFieldSpec(object):
    def __init__(self, name, **params):
        self.name = name
        self.parameters = []
        for key, value in params.items():
            self.parameters.append(Parameter(name=key, value=value))

    def add_parameter(self, name, value):
        self.parameters.append(Parameter(name=name, value=value))


class XfileMapSpec(object):
    def __init__(self, name, lookup_source_name):
        self.name = name
        self.lookup_source = lookup_source_name
        self.fields = []

    def add_field(self, name, **params):
        self.fields.append(XfileFieldSpec(name, **params))

    def add_field_specs(self, fieldspec_array):
        self.fields.extend(fieldspec_array)


class DatasourceSpec(object):
    def __init__(self, name, classname):
        self.name = name
        self.classname = classname
