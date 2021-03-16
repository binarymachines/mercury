#!/usr/bin/env python


from abc import ABC, abstractmethod


class JSONRecordToCSVConverter(object):
    def __init__(self, *field_names, default_null_value=None):
        self.field_names = field_names
        self.default_null = default_null_value

        
    def create_row(self, json_record):
        return [json_record.get(field, self.default_null) for field in self.field_names]


    def convert_json_record(self, json_rec, delimiter) -> list:
        return delimiter.join(self.create_row(json_rec))


class JSONDocToCSVConverter(ABC):
    
    @abstractmethod
    def get_field_names(self, json_document: dict, **kwargs) -> list:
        pass

    @abstractmethod
    def scan(self, json_document: dict, field_names: list, **kwargs) -> dict:
        pass

    def convert(self, json_document: dict, delimiter: str, should_print_header: bool=True, **kwargs) -> list:

        field_names = self.get_field_names(json_document, **kwargs)
        if should_print_header:
            yield delimiter.join(field_names)

        for record in self.scan(json_document, field_names, **kwargs):            
            row = [str(record.get(name)) for name in field_names]
            yield(delimiter.join(row))

    