#!/usr/bin/env python

import re


class S3Key(object):
    def __init__(self, s3_key_string):
        self.folder_path = self.extract_folder_path(s3_key_string)
        self.object_name = self.extract_object_name(s3_key_string)
        self.full_name = s3_key_string


    def extract_folder_path(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return ''
        key_tokens = s3_key_string.split('/')
        return '/'.join(key_tokens[0:-1])


    def extract_object_name(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return s3_key_string
        return s3_key_string.split('/')[-1]




class RecordCheckErrorCode(object):
    @staticmethod
    def missing_field(field_name):
        return 'MISSING_FIELD "%s"' % field_name


    @staticmethod
    def bad_field_format(field_name, field_data):
        return 'BAD_FIELD_FORMAT: data"%s" in field "%s"' % (field_data, field_name)



class RecordCheckStatus(object):
    def __init__(self, error_codes=[]):
        self.error_codes = error_codes


    @staticmethod
    def ok():
        return RecordCheckStatus()

    @staticmethod
    def error(error_codes):
        return RecordCheckStatus(error_codes)


    def is_ok(self):
        return len(self.error_codes) == 0


    def get_errors(self):
        return self.error_codes


    def __str__(self):
        if self.is_ok():
            return 'record OK'
        return 'errors in record: %s' % (', '.join(self.error_codes))


class FieldRule(object):
    type_regex_map = {'String': r'^[a-zA-Z0-9][ A-Za-z0-9_-]*$',
                      'Integer': r'\b[0-9]+\b(?!\.[0-9])',
                      'Float': r'([0-9]*\.[0-9]+|[0-9]+)'}

    def __init__(self, **kwargs):
        self._is_required = True
        required = kwargs.get('required')
        if not required or required == 'False':
            self._is_required = False
        self._format_regex = None
        regex_string = kwargs.get('format_regex_string')
        if regex_string:
            self._format_regex = re.compile(regex_string)
        self._type_name = kwargs.get('datatype')


    def says_field_is_required(self):
        return self._is_required


    def says_data_is_valid(self, data):
        if not self._format_regex:
            return True

        if self._format_regex.match(str(data)):
            return True
        return False



class TextRecordValidationProfile(object):
    def __init__(self, type_name, schema_config):
        self.field_rules = {}
        self.record_type = type_name
        for field_name in schema_config:
            field_is_required = schema_config[field_name].get('required')
            field_type = schema_config[field_name].get('type')
            if field_type.lower() == 'String'.lower():
                field_regex = schema_config[field_name].get('format_regex_string')
            else:
                field_regex = FieldRule.type_regex_map.get(field_type)
            field_type = schema_config[field_name].get('datatype')
            self.field_rules[field_name] = FieldRule(format_regex_string=field_regex,
                                                     datatype=field_type,
                                                     required=field_is_required)


    def check_record(self, record):
        if not record:
            raise Exception('cannot verify a null record.')

        errors = []
        for field_name, field_rule in self.field_rules.iteritems():
            if not record.get(field_name):
                if field_rule.says_field_is_required():
                    errors.append(RecordCheckErrorCode.missing_field(field_name))
            else:
                field_data = record[field_name]
                if not field_rule.says_data_is_valid(field_data):
                    errors.append(RecordCheckErrorCode.bad_field_format(field_name, field_data))

        if len(errors):
            return RecordCheckStatus.error(errors)

        return RecordCheckStatus.ok()



class CSVLineValidationProfile(object):
    def __init__(self, delimiter, num_fields, text_qualifier=None):
        self.delimiter = delimiter
        self.num_fields = num_fields
        self.text_qualifier = text_qualifier


    def check(self, input_line):
        checks_remaining = 2
        if self.text_qualifier:
            if input_line.count(self.text_qualifier) % 2 == 0:
                checks_remaining -= 1
        else:
            checks_remaining -= 1

        num_fields_in_line = len(input_line.split(self.delimiter))
        if num_fields_in_line == self.num_fields:
            checks_remaining -= 1

        if checks_remaining:
            return False
        return True




class Scanner(object):
    def __init__(self, validation_profile):
        pass


