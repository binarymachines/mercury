#!/usr/bin/env python




class BadDatasource(object):
    def __init__(self, service_object_registry):
        pass



class SampleDatasource(object):
    def __init__(self, service_object_registry):
        self.called_composite_id_func = False


    def lookup_widget_composite_id(self, target_field_name, source_record, field_value_map):
        self.called_composite_id_func = True
        name = source_record.get('NAME')
        if not name:
            name = source_record.get('ALIAS')
        catalog_id = source_record.get('SKU')
        return '%s_%s' % (name, catalog_id)


    