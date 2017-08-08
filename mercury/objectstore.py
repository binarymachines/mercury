#/usr/bin/env python


import yaml
import os
import logging
import sqlalchemy as sqla
import sqlalchemy_utils

from snap import snap
from snap import common
import sqldbx as sqlx
import telegraf as tg
import csvutils


object_table_create_template = '''
CREATE TABLE IF NOT EXISTS "{schema}"."{tablename}" (
"{pk_field}" {pk_type} NOT NULL {pk_default},
{fields},
PRIMARY KEY ("{pk_field}")
)
'''

load_query_template = '''
SELECT
{fields}
FROM "{schema}"."{table}"
WHERE generation = {generation}
'''

insert_statement_template = '''
INSERT INTO "{schema}"."{table}"
(
    {fields}
)
VALUES
(
    {placeholders}
)
'''


class FieldSpec(object):
    def __init__(self, field_name, sql_type_name, *field_args):
        self._name = field_name
        self._sqltype = sql_type_name
        self._field_args = field_args


    def get_field_arg_clause(self):
        if len(self._field_args):
            return ' '.join(self._field_args)
        return ''


    @property
    def name(self):
        return self._name


    @property
    def sqltype(self):
        return self._sqltype


    @property
    def sql(self):
        return '"{fname}" {ftype} {args}'.format(ftype=self.sqltype, 
                                                 fname=self.name,
                                                 args=self.get_field_arg_clause())



class TableSpec(object):
    def __init__(self, name, **kwargs):
        kwreader = common.KeywordArgReader('pk_field',
                                            'pk_type',
                                            'schema',
                                            'object_id_field')
        kwreader.read(**kwargs)
        self._table_name = name
        self._fact_id_field = kwreader.get_value('object_id_field')
        self._primary_key_field = kwreader.get_value('pk_field')
        self._primary_key_type = kwreader.get_value('pk_type')
        self._schema = kwreader.get_value('schema')
        pk_default = kwreader.get_value('pk_default')
        if pk_default:
            self._pk_default_clause = 'DEFAULT %s' % pk_default
        else:
            self._pk_default_clause = ''
        self._data_fields = []
        self._meta_fields = []
        self._transform_map = {}


    def _generate_placeholders(self, fieldnames):
        return [':%s' % f for f in fieldnames]


    @property
    def insert_statement_template(self):
        target_fields = []
        target_fields.extend(self.data_fieldnames)
        target_fields.extend(self.meta_fieldnames)

        placeholders = self._generate_placeholders(target_fields)
        return insert_statement_template.format(schema=self.schema,
                                                table=self.tablename,
                                                fields=',\n'.join(target_fields),
                                                placeholders=',\n'.join(placeholders))
                                                                            

    def add_data_field(self, field_spec):
        self._data_fields.append(field_spec)


    def add_meta_field(self, field_spec):
        self._meta_fields.append(field_spec)


    def get_field_creation_sql(self):
        all_fields = self._data_fields + self._meta_fields
        create_lines = [f.sql for f in all_fields]
        return ',\n'.join(create_lines)

    @property
    def fact_id_field(self):
        return self._fact_id_field

    @property
    def data_fields(self):
        fields = {}
        for fieldspec in self._data_fields:
            fields[fieldspec.name] = fieldspec.sqltype
        return fields

    @property
    def data_fieldnames(self):
        return [fieldspec.name for fieldspec in self._data_fields]

    @property
    def meta_fieldnames(self):
        return [fieldspec.name for fieldspec in self._meta_fields]

    @property
    def fieldnames(self):
        result = []
        result.append(self.pk_field_name)
        result.extend(self.data_fieldnames)
        result.extend(self.meta_fieldnames)
        return result    

    @property
    def schema(self):
        return self._schema

    @property
    def tablename(self):
        return self._table_name

    @property
    def pk_field_name(self):
        return self._primary_key_field

    @property
    def pk_field_type(self):
        return self._primary_key_type
    
    @property
    def sql(self):
        return object_table_create_template.format(schema=self._schema,
                                                   tablename=self.tablename,
                                                   pk_field=self.pk_field_name,
                                                   pk_type=self.pk_field_type,
                                                   fields=self.get_field_creation_sql(),
                                                   pk_default=self._pk_default_clause)


    def add_to_transform_map(self, f_name, f_type, **kwargs):
        if f_name in self._transform_map:
            raise Exception(self, 'Name %s already exists in TableSpecBuilder transform map' % f_name)
        converter = None
        if 'int' in f_type:
            converter = csvutils.StringToIntConverter()
        elif 'float' in f_type:
            converter = csvutils.StringToFloatConverter()
        elif 'bool' in f_type:
            converter = csvutils.StringToBooleanConverter()
        elif 'date' in f_type:
            str_format = kwargs.get('string_format', '%Y-%m-%d %H:%M:%S')
            converter = csvutils.StringToDatetimeConverter() # Add format string as converter param if needed
        elif 'varchar' not in f_type:
            raise Exception(self, 'Type %s is not recognized for the transform map' % f_type)

        if converter:
            self._transform_map[f_name] = converter

    def convert_data(self, data):
        converted_data = {}
        for key, value in data.iteritems():
            if key in self._transform_map:
                new_value = self._transform_map[key].convert(value)
                converted_data[key] = new_value
            else:
                converted_data[key] = value
        return converted_data


class TableSpecBuilder(object):
    def __init__(self, table_name, **kwargs):
        self._name = table_name
        self._extra_params = {}
        for key, value in kwargs.iteritems():
            self._extra_params[key] = value

        self._tablespec = TableSpec(self._name, **kwargs)
        self._transform_map = {}


    def add_data_field(self, f_name, f_type, *field_args):
        args = ['NOT NULL']
        args.extend(field_args)
        self._tablespec.add_data_field(FieldSpec(f_name, f_type, *args))
        self._tablespec.add_to_transform_map(f_name, f_type, **self._extra_params)
        return self


    def add_meta_field(self, f_name, f_type, *field_args):
        self._tablespec.add_meta_field(FieldSpec(f_name, f_type, *field_args))
        return self


    def build(self):
        return self._tablespec
    


class Accumulator(object):
    def __init__(self, source_tablespec, source_sqldb):
        self._src_tablespec = source_tablespec
        self._src_pmgr = sqlx.PersistenceManager(source_sqldb)


    @property
    def tablespec(self):
        return self._src_tablespec

    @property
    def persistence_mgr(self):
        return self._src_pmgr


    def _load(self, loading_query, **kwargs):
        '''override in subclass'''
        raise Exception('The _load() method must be overridden in a subclass of Accumulator.')


    def clear(self):
        '''remove all staged data -- override in subclass'''


    def generate_load_query(self, generation_number, **kwargs):
        return load_query_template.format(fields=',\n'.join(self._src_tablespec.data_fieldnames),
                                          schema=self._src_tablespec.schema,
                                          table=self._src_tablespec.tablename,
                                          generation=generation_number)
        

    def load_source_data(self, **kwargs):
        kwreader = common.KeywordArgReader()
        kwreader.read(**kwargs)

        max_generation = int(kwreader.get_value('max_generation') or 0)
        current_gen = 0
        load_error = None
        while True:
            loading_query = self.generate_load_query(current_gen)
            try:
                self._load(loading_query, **kwargs)
                if max_generation >= 0 and current_gen == max_generation:
                    break
                current_gen += 1 
            except Exception, err:
                self.clear()
                load_error = err
                break
        if load_error:
            raise load_error
                


    def backfill(self, source_table_spec, reading_frame, generation):
        pass


    def push_source_data(self, kafka_loader):
        '''override in subclass'''
        pass

 
class InMemoryAccumulator(Accumulator):
    def __init__(self, tablespec, source_sqldb):
        Accumulator.__init__(self, tablespec, source_sqldb)
        self._data = {}


    def _load(self, loading_query, **kwargs):
        with sqlx.txn_scope(self.persistence_mgr.database) as session:
            results = session.execute(loading_query)
            for row in results:
                record_id = row[self.tablespec.fact_id_field]
                #row2dict = lambda row: {c.name: str(getattr(row, c.name)) for c in row.__table__.columns}
                self._data[record_id] = dict(row)
        

    def get_data(self):
        return self._data


    def clear(self):
        self._data.clear()


class ObjectstoreConfig(object):
    def __init__(self, configfile_name, **kwargs):
        yaml_config = None
        with open(configfile_name) as f:
            yaml_config = yaml.load(f)

        self._cluster = tg.KafkaCluster()
        for entry in yaml_config['globals']['cluster_nodes']:
            tokens = entry.split(':')
            ip = tokens[0]
            port = tokens[1]
            self._cluster = self._cluster.add_node(tg.KafkaNode(ip, port))

        self._source_topic = yaml_config['globals']['source_topic']

        service_object_tbl = snap.initialize_services(yaml_config, logging.getLogger(__name__))
        self._service_object_registry = common.ServiceObjectRegistry(service_object_tbl)

        object_db_config = yaml_config['object_db']
        so_name = object_db_config['service_object']

        db_service_object = self._service_object_registry.lookup(so_name)
        db_property = object_db_config['property']

        self._db = getattr(db_service_object, db_property)

        tbl_name = yaml_config['tablespec']['table_name']
        target_schema = yaml_config['tablespec']['schema']
        obj_id_field = yaml_config['tablespec']['object_id_field']
        pkfield_cfg = yaml_config['tablespec']['pk_field']
        tspec_builder = TableSpecBuilder(tbl_name,
                                         schema=target_schema,
                                         pk_field=pkfield_cfg['name'],
                                         pk_type=pkfield_cfg['type'],
                                         object_id_field=obj_id_field,
                                         pk_default=pkfield_cfg['default'])

        for fieldname in yaml_config['tablespec']['data_fields']:
            fieldtype = yaml_config['tablespec']['data_fields'][fieldname]
            tspec_builder.add_data_field(fieldname, fieldtype)

        for fieldname in yaml_config['tablespec']['meta_fields']:
            fieldtype = yaml_config['tablespec']['meta_fields'][fieldname]
            tspec_builder.add_meta_field(fieldname, fieldtype)

        self._tablespec = tspec_builder.build()


    @property
    def source_topic(self):
        return self._source_topic

    @property
    def cluster(self):
        return self._cluster

    @property
    def database(self):
        return self._db

    @property
    def tablespec(self):
        return self._tablespec

    @property
    def kafka_cluster(self):
        return self._cluster


class TimelimeExtractor(object):
    def __init__(self):
        pass


class TimeFrame(object):
    def __init__(self, start_date, end_date, **kwargs):
        pass







