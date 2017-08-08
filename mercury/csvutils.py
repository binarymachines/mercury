#!/usr/bin/env python


import common
import arrow
from datetime import datetime


class MethodNotImplementedError(Exception):
    def __init__(self, method_name, klass):
        Exception.__init__(self, 'Method %s(...) in class "%s" is not implemented. Please check your subclass(es).' % (method_name, klass.__name__))

        
        
class NoDataForFieldInSourceRecordError(Exception):
    def __init__(self, field_name, record):
        Exception.__init__(self, 'Data for field "%s" not present in source record: %s' % (field_name, str(record)))



class IncorrectConverterTypeError(Exception):
    def __init__(self, source_class, target_class):
        Exception.__init__(self, 'Tried to pass an object of type %s to a converter which handles type %s.' % (target_class.__name__, source_class.__name__))
        

class NoSuchCSVFieldException(Exception):
    def __init__(self, field_name):
        Exception.__init__(self, 'No field in csv record map named "%s"' % field_name)


class DuplicateCSVFieldNameException(Exception):
    def __init__(self, field_name):
        Exception.__init__(self, 'CSV field %s already exists in CSV Record Map Builder.' % field_name)


        
class CSVField(object):
    def __init__(self, name, field_type):
        self.name = name
        self.type = field_type



class CSVDataConverter(object):
    def __init__(self, source_class):
        self.source_class = source_class

        
    def _convert(self, obj):
        raise MethodNotImplementedError('_convert', self.__class__)


    def convert(self, obj):
        '''
        if not issubclass(self.source_class, obj.__class__):
            raise IncorrectConverterTypeError(self.source_class, object.__class__)
        '''
        return self._convert(obj)

    
class TimestampISOConverter(CSVDataConverter):
    def __init__(self):
        CSVDataConverter.__init__(self, datetime)
    
    def _convert(self, obj):
        return '"%s"' %  str(arrow.get(obj).format('YYYY-MM-DD HH:MM:SS'))
    


class DatetimeStringToISOFormatConverter(CSVDataConverter):
    def __init__(self):
        CSVDataConverter.__init__(self, str)

        
    def _convert(self, obj):
        return arrow.get(obj).isoformat()

    
        
class StringToBooleanConverter(CSVDataConverter):

    def __init__(self):
        CSVDataConverter.__init__(self, str)

    def _convert(self, obj):
        if obj == 't' or obj == 'true' or obj == 'True':
            return True
        elif obj == 'f' or obj == 'false' or obj == 'False':
            return False
        else:
            return None


class StringToDatetimeConverter(CSVDataConverter):

    def __init__(self, str_format):
        CSVDataConverter.__init__(self, datetime)
        self._format = str_format


    def _convert(self, obj):
        return datetime.strptime(obj, self._format)
        

class StringToIntConverter(CSVDataConverter):
    def __init__(self):
        CSVDataConverter.__init__(self, str)


    def _convert(self, obj):
        return int(obj)
    

class StringToFloatConverter(CSVDataConverter):
    def __init__(self):
        CSVDataConverter.__init__(self, str)


    def _convert(self, obj):
        return float(obj)

    
    
        
class CSVRecordMap(object):
    def __init__(self, field_array, conversion_tbl={}, **kwargs):
        self.delimiter = kwargs.get('delimiter', ',')
        self.fields = field_array
        self.conversion_tbl = conversion_tbl
        

    def header(self, **kwargs):
        output = []
        for f in self.fields:
            output.append(f.name)

        delimiter = kwargs.get('delimiter') or self.delimiter
        return delimiter.join(output)
    

    def format(self, data, field):
        if field.type.__name__ in ['str', 'unicode']:
            #result = '"%s"' % data            
            return data.encode("utf-8")
        return str(data)


    def row_to_dictionary(self, row, **kwargs):
        row = row.strip()
        should_accept_nulls = kwargs.get('accept_nulls', False)
        output = {}
        index = 0
        tokens = row.split(self.delimiter)
        if len(tokens) != len(self.fields):
            raise Exception('Mismatch between number of defined fields and number of fields in row: %s' % row)

        for token in tokens:
            current_field = self.fields[index]
            field_name = current_field.name
            field_value = None
            raw_field_data = token
            if raw_field_data == '' and not should_accept_nulls:
                raise NoDataForFieldInSourceRecordError(field_name, row)
            elif raw_field_data == '':
                field_value = None

            if self.conversion_tbl.get(field_name):
                field_value = self.conversion_tbl[field_name].convert(raw_field_data)
            else:
                field_value = self.format(raw_field_data, current_field)
            
            output[field_name] = field_value
            index += 1

        return output
        
    
    
    def dictionary_to_row(self, dict, **kwargs):
        should_accept_nulls = kwargs.get('accept_nulls', True)
        output = []
        for f in self.fields:
            data = dict.get(f.name)
            if data is None and not should_accept_nulls:
                raise NoDataForFieldInSourceRecordError(f.name, dict)
            elif data is None:
                data = ''
                
            if self.conversion_tbl.get(f.name):
                output.append(self.conversion_tbl[f.name].convert(dict.get(f.name)))
            else:
                output.append(self.format(data, f))

        delimiter = kwargs.get('delimiter') or self.delimiter
        return delimiter.join(output)


    def convert_dict(self, row_dict, **kwargs):
        should_accept_nulls = kwargs.get('accept_nulls', True)
        output = {}
        for f in self.fields:
            data = row_dict.get(f.name)
            if data is None and not should_accept_nulls:
                raise NoDataForFieldInSourceRecordError(f.name, row_dict)
            elif data is None:
                data = ''

            if self.conversion_tbl.get(f.name):
                output[f.name] = self.conversion_tbl[f.name].convert(row_dict.get(f.name))
            else:
                output[f.name] = self.format(data, f)

        return output


    

    
class CSVRecordMapBuilder(object):
    def __init__(self):
        self.fields = []
        self.converter_map = {}
        self.field_names = set()


    def register_converter(self, data_converter, field_name):
        if field_name not in self.field_names:
            raise NoSuchCSVFieldException(field_name)
        
        self.converter_map[field_name] = data_converter
        return self


    def add_field(self, field_name, datatype):
        if field_name in self.field_names:
            raise DuplicateCSVFieldNameException(field_name)
        
        self.fields.append(CSVField(field_name, datatype))
        self.field_names.add(field_name)
        return self


    def build(self, **kwargs):
        # try to automatically determine what converters we'll need
        for f in self.fields:
            if f.type == int or 'int' in f.type:
                self.register_converter(StringToIntConverter(), f.name)
            if f.type == float or 'float' in f.type:
                self.register_converter(StringToFloatConverter(), f.name)
            if f.type == bool or 'bool' in f.type:
                self.register_converter(StringToBooleanConverter(), f.name)
            if f.type == datetime or 'date' in f.type:
                str_format = kwargs.get('string_format', '%Y-%m-%d %H:%M:%S')
                self.register_converter(StringToDatetimeConverter(str_format), f.name)
              
        return CSVRecordMap(self.fields, self.converter_map, **kwargs)

