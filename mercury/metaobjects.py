#!/usr/bin/env python

import json
import copy
from collections import namedtuple
from collections import OrderedDict
from snap import common

REQUIRED_ED_CHANNEL_FIELDS = ['handler_function',
                           'table_name',
                           'operation',
                           'primary_key_field',
                           'primary_key_type',
                           'payload_fields']


REQUIRED_ED_GLOBAL_FIELDS = ['project_directory',
                          'database_host',
                          'database_name',
                          'debug',
                          'handler_module']


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


class EavesdropprChannelSpec(object):
    def __init__(self, name, **kwargs):
        kwreader = common.KeywordArgReader(*REQUIRED_ED_CHANNEL_FIELDS)
        kwreader.read(**kwargs)
        self.name = name
        self._data = {}
        for key, value in kwargs.items():
            if key == 'payload_fields':
                self._data[key] = set()
                # here we know that value is actually a collection
                for field in value:
                    self._data[key].add(field)
            else:
                self._data[key] = value


    def rename(self, new_name):
        new_data = copy.deepcopy(self._data)
        return EavesdropprChannelSpec(new_name, **new_data)


    def set_property(self, name, value):
        new_data = copy.deepcopy(self._data)
        new_data[name] = value
        return EavesdropprChannelSpec(self.name, **new_data)


    def property_names(self):
        return self._data.keys()


    @property
    def handler_function(self):
        return self._data['handler_function']


    @property
    def schema(self):
        return self._data.get('schema', 'public')


    @property
    def table_name(self):
        return self._data['table_name']


    @property
    def operation(self):
        return self._data['operation']

    @property
    def primary_key_field(self):
        return self._data['primary_key_field']


    @property
    def primary_key_type(self):
        return self._data['primary_key_type']


    @property
    def procedure_name(self):
        return self._data['procedure_name']


    @property
    def trigger_name(self):
        return self._data.get('trigger_name')


    @property
    def payload_fields(self):
        return self._data['payload_fields']


    def add_payload_fields(self, *fields):
        new_data = copy.deepcopy(self._data)
        for field in fields:
            new_data['payload_fields'].add(field)
        return EavesdropprChannelSpec(self.name, **new_data)


    def delete_payload_field(self, field):
        new_data = copy.deepcopy(self._data)
        new_data['payload_fields'].discard(field)
        return EavesdropprChannelSpec(self.name, **new_data)


    def data(self):
        result = {}
        for key, value in self._data.items():
            if key == 'payload_fields':
                result[key] = []
                for f in value:
                    result[key].append(f)
            else:
                result[key] = value

        return result



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
        self.alias = name
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
            'alias': self.alias,
            'init_params': [
                {'name': p.name, 'value': p.value} for p in self.init_params
            ]
        }
    
class NgstTarget(object):
    def __init__(self, name, datastore_name, checkpoint_interval):
        self.name = name
        self.datastore_alias = datastore_name
        self.checkpoint_interval = checkpoint_interval

    def data(self):
        return {
            'name': self.name,
            'datastore': self.datastore_alias,
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


class CyclopsTrigger(object):
    def __init__(self, name, event_type, parent_dir, file_filter, function):        
        # valid event_types: created | updated
        self.name = name
        self.event_type = event_type
        self.parent_directory = parent_dir
        self.filename_filter = file_filter
        self.handler_func = function

    def data(self):
        return {
            'name': self.name,
            'event_type': self.event_type,
            'parent_dir': self.parent_directory,
            'filename_filter': self.filename_filter,
            'function': self.handler_func
        }

class DFProcessorSpec(object):
    def __init__(self, name, read_func, transform_func, write_func):
        self.name = name
        self.read_func = read_func
        self.transform_func = transform_func
        self.write_func = write_func

    def data(self):
        return {
            'name': self.name,
            'read_function': self.read_func,
            'transform_function': self.transform_func,
            'write_function': self.write_func
        }


class J2SqlGenDefaultsSpec(object):
    def __init__(self, **kwargs):
        self.required_settings = [
            'autocreate_pk_if_missing',
            'pk_name',
            'pk_type',
            'varchar_length',
            'column_type_map'
        ]
        self.optional_settings = [
            'table_suffix',
            'column_suffix'
        ]

        kwreader = common.KeywordArgReader(*self.required_settings)
        kwreader.read(**kwargs)
        self.settings = OrderedDict()
        self.column_type_map = {}
        for name in self.required_settings:
            if name == 'column_type_map':
                for k,v in kwargs['column_type_map'].items():
                    self.column_type_map[k] = v
            else:
                self.settings[name] = Parameter(name=name, value=kwreader.get_value(name))
            kwargs.pop(name)
        
        for key, value in kwargs.items():
            param = Parameter(name=key, value=value)
            self.settings[key] = param

        self.settings['table_suffix'] = Parameter(name='table_suffix',
                                                  value=kwargs.get('table_suffix', ''))
        self.settings['column_suffix'] = Parameter(name='column_suffix',
                                                  value=kwargs.get('column_suffix', ''))


    def add_setting(self, name, value):
        self.settings[name] = Parameter(name=name, value=value)

    def set_table_suffix(self, value):
        self.table_suffix = value

    def set_column_suffix(self, value):
        self.column_suffix = value


class J2SqlGenTableMapSpec(object):
    def __init__(self, tablename, rename_to=None):
        self.table_name = tablename
        self.rename_to = rename_to

        # this is {old_name: new_name} for every column to be renamed
        self.column_rename_map = {}

        # the key is column name, value is {setting_name: setting_value}
        self.column_settings = {}


    def set_rename_target(self, name):
        self.rename_to = name


    def remap_column(self, old_colname, new_colname):
        self.column_rename_map[old_colname] = new_colname


    def add_column_settings(self, column_name, **kwargs):
        settings = self.column_settings.get(column_name, {})
        settings.update(kwargs)
        self.column_settings[column_name] = settings


class PGExecTargetSpec(object):
    def __init__(self, name, host, port, user, password, **kwargs):
        self.name = name
        self.host = host
        self.port = port
        self.username = user
        self.password = password
        self.settings = OrderedDict()

        for key, value in kwargs.items():
            self.settings[key] = meta.Parameter(name=key, value=value)            


    def update_settings(self, name, value):
        self.settings[key] = meta.Parameter(name=name, value=value)


