#!/usr/bin/env python


import sys
import csv
from snap import snap, common
import yaml
import sqldbx as sqlx
import logging
import csvutils


class NoSuchTargetFieldException(Exception):
    def __init__(self, field_name):
        Exception.__init__(self,
                           'RecordTransformer does not contain the target field %s.' % field_name)


class NoDatasourceForFieldException(Exception):
    def __init__(self, field_name):
        Exception.__init__(self,
                           'No datasource registered for target field name %s.' % field_name)


class NoSuchLookupMethodException(Exception):
    def __init__(self, datasource_classname, method_name):
        Exception.__init__(self,
                           'Registered datasource %s has no lookup method "%s(...)' % (datasource_classname, method_name))


class NonexistentDatasourceException(Exception):
    def __init__(self, src_name, module_name):
        Exception.__init__(self, 'No datasource named "%s" in target Python module "%s".' % (src_name, module_name))


class MissingSupplierMethodException(Exception):
    def __init__(self, classname, methodname):
        Exception.__init__(self, 'DataSupplier subclass %s contains no supply method "%s".' % (classname, methodname))


class InvalidSupplierRequestException(Exception):
    def __init__(self, field_name):
        Exception.__init__(self, 'Cannot supply a value for a record identifier field (%s).' % field_name)


class InvalidSupplierRecordException(Exception):
    def __init__(self, record_id_field_name):
        Exception.__init__(self, 'Incoming record is missing a value for its identifier field "%s"; cannot supply data.' % record_id_field_name)



class FieldValueResolver(object):
    def __init__(self, field_name_string):
        self._field_names = field_name_string.split('|')

    def resolve(self, source_record):
        for name in self._field_names:
            if source_record.get(name):
                return source_record[name]



class ConstValueResolver(object):
    def __init__(self, value):
        self._value = value

    def resolve(self, source_record):
        return self._value



class FieldValueMap(object):
    def __init__(self):
        self._resolvers = {}


    def add_resolver(self, field_value_resolver, field_name):
        self._resolvers[field_name] = field_value_resolver


    def get_value(self, field_name, source_record):
        resolver = self._resolvers.get(field_name)
        if not resolver:
            raise Exception('No FieldValueResolver registered with value map under the name "%s".' % field_name)
        return resolver.resolve(source_record)


class DataTypeTransformer:
    def __init__(self, log):
        self._csv_record_map_builder = csvutils.CSVRecordMapBuilder()
        self._csv_record_map = None
        self._log = log

    def build(self, type_dict, **kwargs):
        for f_name, f_type in type_dict.iteritems():
            self._csv_record_map_builder.add_field(f_name, f_type)

        self._csv_record_map = self._csv_record_map_builder.build(**kwargs)


    def transform(self, source_record, **kwargs):
        txfrmd_record = self._csv_record_map.convert_dict(source_record, **kwargs)
        log = self._log
        log.debug('Transforming record.')
        log.debug(txfrmd_record)
        return txfrmd_record
    

class RecordTransformer:
    def __init__(self):
        self.target_record_fields = set()
        self.datasources = {}
        self.field_map = {}
        self.value_map = FieldValueMap()


    def add_target_field(self, target_field_name):
        self.target_record_fields.add(target_field_name)


    def map_source_to_target_field(self, source_field_designator, target_field_name):
        if not target_field_name in self.target_record_fields:
            raise NoSuchTargetFieldException(target_field_name)
        resolver = FieldValueResolver(source_field_designator)
        self.field_map[target_field_name] = resolver
        self.value_map.add_resolver(resolver, target_field_name)


    def map_const_to_target_field(self, target_field_name, value):
        if not target_field_name in self.target_record_fields:
            raise NoSuchTargetFieldException(target_field_name)
        self.field_map[target_field_name] = ConstValueResolver(value)


    def register_datasource(self, target_field_name, datasource):
        if not target_field_name in self.target_record_fields:
            raise Exception('No target field "%s" has been added to the transformer.' % target_field_name)
        self.datasources[target_field_name] = datasource


    def lookup(self, target_field_name, source_record):
        record_value = source_record.get(target_field_name)
        if record_value != None and record_value != '':
            return record_value
        datasource = self.datasources.get(target_field_name)
        if not datasource:
            raise NoDatasourceForFieldException(target_field_name)

        return datasource.lookup(target_field_name, source_record, self.value_map)


    def transform(self, source_record, **kwargs):
        target_record = {}
        for key, value in kwargs.iteritems():
            target_record[key] = value

        for target_field_name in self.target_record_fields:
            if self.datasources.get(target_field_name):
                target_record[target_field_name] = self.lookup(target_field_name, source_record)
            elif self.field_map.get(target_field_name):
                source_field_resolver = self.field_map[target_field_name]
                target_record[target_field_name] = source_field_resolver.resolve(source_record)
        
        return target_record



class RecordTransformerBuilder(object):
    def __init__(self, yaml_config_filename, **kwargs):

        self._map_name = kwargs.get('map_name')
        if not self._map_name:
            raise Exception('Missing keyword argument: "map_name"')

        self._transform_config = {}
        with open(yaml_config_filename) as f:
            self._transform_config = yaml.load(f)


    def load_datasource(self, src_name, transform_config, service_object_registry):

        src_module_name = self._transform_config['globals']['lookup_source_module']
        datasource_class_name = self._transform_config['sources'][src_name]['class']
        klass = common.load_class(datasource_class_name, src_module_name)        
        #init_params = self._transform_config['sources'][src_name].get('init_params', {})
        return klass(service_object_registry)


    def build(self):
        service_object_dict = snap.initialize_services(self._transform_config, logging.getLogger())
        so_registry = common.ServiceObjectRegistry(service_object_dict)

        datasource_name = self._transform_config['maps'][self._map_name]['lookup_source']
        datasource = self.load_datasource(datasource_name, self._transform_config, so_registry)
        transformer = RecordTransformer()

        for fieldname in self._transform_config['maps'][self._map_name]['fields']:
            transformer.add_target_field(fieldname)

            field_config = self._transform_config['maps'][self._map_name]['fields'][fieldname]

            if field_config['source'] == 'record':
                transformer.map_source_to_target_field(field_config['key'], fieldname)
            elif field_config['source'] == 'lookup':
                transformer.register_datasource(fieldname, datasource)
            elif field_config['source'] == 'value':
                transformer.map_const_to_target_field(fieldname, field_config['value'])
            else:
                raise Exception('unrecognized source type "%s." Allowed types are record, lookup, and value.' % field_config['source'])                

        return transformer


    @property
    def config(self):
        return self._transform_config


class DataProcessor(object):
    def __init__(self, processor=None):
        self._processor = processor


    def _process(self, record):
        pass


    def process(self, record):
        if self._processor:
            data = self._processor.process(record)
        else:
            data = record
        return self._process(data)



class ConsoleProcessor(DataProcessor):
    def __init__(self, processor=None):
        DataProcessor.__init__(self, processor)

    def _process(self, record):
        print common.jsonpretty(record)
        return record



class SQLTableInsertProcessor(DataProcessor):
    def __init__(self, sql_db, insert_function, processor=None, **kwargs):
        DataProcessor.__init__(self, processor)
        self._insert_function = insert_function
        self._db = sql_db


    def _process(self, record):
        self._insert_function(record, self._db)



class WhitespaceCleanupProcessor(DataProcessor):
    def __init__(self, processor=None):
        DataProcessor.__init__(self, processor)


    def _process(self, record):
        data = {}
        for key, value in record.iteritems():
            data[key] = value.strip()
        return data


class NullByteFilter:
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('delimiter', 'field_names')
        kwreader.read(**kwargs)
        self._delimiter = kwreader.get_value('delimiter')
        self._field_names = kwreader.get_value('field_names')

    def filter_with_null_output(self, src_file_path):
        null_byte_lines_and_fields = []
        with open(src_file_path, 'r') as datafile:
            for line_num, line in enumerate(datafile.readlines()):
                if '\0' in line:
                    null_index = line.find('\0')
                    field_index = line[:null_index].count(self._delimiter)
                    field = self._field_names[field_index]
                    null_byte_lines_and_fields.append((line_num, field))
        return null_byte_lines_and_fields


    def filter_with_readable_output(self, src_file_path):
        readable_lines = []
        with open(src_file_path, 'r') as datafile:
            for record in datafile.readlines():
                if '\0' not in record:
                    readable_lines.append(record)
        return readable_lines



class DataSupplier(object):
    def __init__(self, service_object_registry, **kwargs):
        kwreader = common.KeywordArgReader('record_id_field')
        kwreader.read(**kwargs)
        self._record_id_field = kwreader.get_value('record_id_field')
        self._abort_on_null = kwreader.get_value('abort_on_null') or False
        self.service_object_registry = service_object_registry
        

    def get_service_object(self, so_name):
        return self.service_object_registry.lookup(so_name)


    def supply(self, field_name, record):        
        if field_name == self._record_id_field:
            raise InvalidSupplierRequestException(field_name)

        # record_id = record.get(self._record_id_field)
        # if record_id is None or record_id == '':
        #     raise InvalidSupplierRecordException(self._record_id_field)

        supply_function_name = 'supply_%s' % field_name
        if hasattr(self, supply_function_name):
            supply_function = getattr(self, supply_function_name)

            # we do not need to pass the name of the field whose value
            # is being supplied, because it is implicit in the name of the
            # target method -- if you wrote it, you know what value it should
            # supply

            result = supply_function(record)
            if result is None and self._abort_on_null == True:       
                function_name = sys._getframe().f_code.co_name    
                raise Exception('data supplier method "%s" could not provide a value.' % function_name)
            return result

        else:
            raise MissingSupplierMethodException(self.__class__.__name__, supply_function_name)



class LookupDatasource(object):
    def __init__(self, service_object_registry, **kwargs):
        self._service_object_registry = service_object_registry


    def get_service_object(self, service_object_name):
        return self._service_object_registry.lookup(service_object_name)


    def lookup(self, target_field_name, source_record, field_value_map):
        lookup_method_name = 'lookup_%s' % target_field_name
        if not hasattr(self, lookup_method_name):
            raise NoSuchLookupMethodException(self.__class__.__name, lookup_method_name)
        lookup_method = getattr(self, lookup_method_name)
        return lookup_method(target_field_name, source_record, field_value_map)            



class CSVFileDataExtractor(object):
    def __init__(self, processor=None, **kwargs):
        kwreader = common.KeywordArgReader('quotechar')
        kwreader.read(**kwargs)                                           
        self._delimiter = kwreader.get_value('delimiter') or ','
        self._quote_char = kwreader.get_value('quotechar')        
        self._processor = processor


    def extract(self, filename, **kwargs):
        load_func = kwargs.get('load_function')
        max_lines = int(kwargs.get('max_lines', -1))

        with open(filename, 'rb') as datafile:
            csv_reader = csv.DictReader(datafile,
                                        delimiter=self._delimiter,
                                        quotechar=self._quote_char)
            lines_read = 0
            if not max_lines:
                return

            for record in csv_reader:
                data = record
                if self._processor:
                    data = self._processor.process(record)
                if load_func:
                    load_func(data)
                lines_read += 1
                if max_lines > 0 and lines_read == max_lines:
                    break

