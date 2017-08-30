#!/usr/bin/env python

import unittest
import context

from snap import common
import sys
import logging
import yaml
from sqlalchemy import text

from mercury import sqldbx as sqlx
from mercury import datamap as dmap
from mercury import objectstore as obs

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner

LOG_ID = 'test_objectstore'
TESTING_SCHEMA = 'test'
OBJECT_TABLE_NAME = 'objects'



class TimelineGenerationTest(unittest.TestCase):

    def setUp(self):
        self.log = logging.getLogger(LOG_ID)
        self.env = common.LocalEnvironment('PGSQL_HOST',
                                           'PGSQL_DBNAME',
                                           'PGSQL_USER',
                                           'PGSQL_PASSWORD')
        self.env.init()
        self.db_host = self.env.get_variable('PGSQL_HOST')
        self.db_name = self.env.get_variable('PGSQL_DBNAME')
        self.db_user = self.env.get_variable('PGSQL_USER')
        self.db_password = self.env.get_variable('PGSQL_PASSWORD')

        self.objectstore_db = sqlx.PostgreSQLDatabase(self.db_host, self.db_name)
        self.objectstore_db.login(self.db_user, self.db_password)

        self.tablespec_builder = obs.TableSpecBuilder(OBJECT_TABLE_NAME,
                                                      schema=TESTING_SCHEMA,
                                                      pk_field='id',
                                                      pk_type='uuid',
                                                      object_id_field='event_uuid',
                                                      pk_default='public.gen_random_uuid()')

        self.tablespec_builder.add_data_field('event_type', 'varchar(32)')
        self.tablespec_builder.add_data_field('event_uuid', 'varchar(32)')
        self.tablespec_builder.add_data_field('price', 'float')
        self.tablespec_builder.add_data_field('event_date', 'date')
        self.tablespec_builder.add_meta_field('generation', 'int4')
        self.tablespec_builder.add_meta_field('correction_id', 'int4')

        configfile = 'data/small_objectstore_cfg.yaml'
        self.obj_store_cfg = obs.ObjectstoreConfig(configfile)

        self.create_object_table(self.tablespec_builder.build(), self.objectstore_db)
        self.clear_object_table(self.objectstore_db)


    def tearDown(self):
        #self.destroy_object_table(self.objectstore_db)
        pass


    def insert_test_record(self, record, target_db):

        insert_sql = text('''
        insert into "test"."objects"
        (EVENT_TYPE, EVENT_UUID, PRICE, EVENT_DATE, GENERATION, CORRECTION_ID)
        values
        (:objtype, :uuid, :price, :date, 0, NULL)
        ''')

        insert_stmt = insert_sql.bindparams(date=record['DATE'],
                                            price=record['PRICE'],
                                            objtype=record['TYPE'],
                                            uuid=record['UUID'])
        with sqlx.txn_scope(target_db) as session:
            session.execute(insert_stmt)


    def load_test_data(self, filename, tablespec):
        cproc = dmap.ConsoleProcessor()
        #insert_proc = dmap.SQLTableInsertProcessor(db, self.insert_test_record, cproc)

        
        extractor = dmap.CSVFileDataExtractor(insert_proc,
                                              quotechar='"',
                                              delimiter='|')
        
        extractor.extract(filename)


    def get_csv_sourcefile_record_count(self, filename):
        '''assume the source file has the first line as its header'''    
        count = 0
        with open(filename) as f:
            f.readline() # discard the header
            while f.readline():
                count += 1
        return count


    def create_object_table(self, tablespec, db):
        with sqlx.txn_scope(db) as session:
            session.execute(tablespec.sql)


    def destroy_object_table(self, db):
        with sqlx.txn_scope(db) as session:
            drop_statement = 'DROP TABLE "%s"."%s"' % (TESTING_SCHEMA, OBJECT_TABLE_NAME)
            session.execute(drop_statement)


    def clear_object_table(self, db):
        with sqlx.txn_scope(db) as session:
            drop_statement = 'TRUNCATE TABLE "%s"."%s"' % (TESTING_SCHEMA, OBJECT_TABLE_NAME)
            session.execute(drop_statement)


    def test_tablespec_generates_correct_sql(self):
        self.destroy_object_table(self.objectstore_db)     
        tablespec = self.tablespec_builder.build()
        self.log.debug(tablespec.sql)
        self.create_object_table(tablespec, self.objectstore_db)
        
        engine = self.objectstore_db.get_engine()
        self.assertTrue(engine.dialect.has_table(engine, 
                                                 tablespec.tablename,
                                                 schema=TESTING_SCHEMA))


    def test_accumulator_generates_correct_loading_query(self):
        '''there is probably a better (less ugly) way to implement this test,
        but for now we settle for making sure the sql create syntax
        specifies the correct set of fields'''

        tablespec = self.tablespec_builder.build()
        sql = tablespec.sql
        index1 = sql.find('(') + 1
        index2 = sql.rfind(',')
        sql[index1:index2]
        tokens = [line.split(' ')[0] for line in sql[index1:index2].split('\n') if line]
        field_count = 0
        match_count = 0
        for t in tokens:
            print '-> %s' % t
            if t.lstrip('"').rstrip('"'):
                field_count += 1
            if t.lstrip('"').rstrip('"') in tablespec.fieldnames:
                match_count += 1

        self.assertEquals(field_count, len(tablespec.fieldnames))
        self.assertEquals(match_count, field_count)


    def test_in_memory_accumulator_generates_correct_dataset(self):
        source_filename = 'data/sample_objectstore.csv'
        self.load_test_data(source_filename, self.objectstore_db)

        tablespec = self.tablespec_builder.build()
        acc = obs.InMemoryAccumulator(tablespec, self.objectstore_db)
        loading_query = acc.generate_load_query(0)
        self.log.debug(loading_query)

        acc.load_source_data(max_generation=0)
        self.log.debug('### dataset:')
        self.log.debug(acc.get_data())

        condensed_record_count = len(acc.get_data().keys())
        records_in_source = self.get_csv_sourcefile_record_count(source_filename)

        self.assertEquals(condensed_record_count,
                          records_in_source - 1)


    def test_tablespec_config_loads_valid_tablespec(self):
        configfile = 'data/sample_objectstore_cfg.yaml'
        
        obj_store_cfg = obs.ObjectstoreConfig(configfile)
        self.log.debug(obj_store_cfg.tablespec.sql)

        tblspec = obj_store_cfg.tablespec
        self.create_object_table(tblspec, obj_store_cfg.database)
        
        engine = obj_store_cfg.database.get_engine()
        self.assertTrue(engine.dialect.has_table(engine, 
                                                 tblspec.tablename,
                                                 schema=tblspec.schema))
                                             
        with sqlx.txn_scope(obj_store_cfg.database) as session:
            drop_statement = 'DROP TABLE "%s"."%s"' % (tblspec.schema, tblspec.tablename)
            session.execute(drop_statement)


    def test_tablespec_config_generates_correct_insert_statement(self):
        configfile = 'data/sample_objectstore_cfg.yaml'
        
        obj_store_cfg = obs.ObjectstoreConfig(configfile)
        self.log.debug(obj_store_cfg.tablespec.sql)

        tblspec = obj_store_cfg.tablespec
        self.create_object_table(tblspec, obj_store_cfg.database)
        self.load_test_data('data/sample_objectstore.csv', obj_store_cfg.database)
        
        self.log.debug(tblspec.insert_statement_template)
                                             
        with sqlx.txn_scope(obj_store_cfg.database) as session:
            drop_statement = 'DROP TABLE "%s"."%s"' % (tblspec.schema, tblspec.tablename)
            session.execute(drop_statement)
        
        self.assertTrue(False)


if __name__ == '__main__':
    logging.basicConfig( stream=sys.stderr )
    logging.getLogger(LOG_ID).setLevel( logging.DEBUG )

    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
        
    unittest.main(testRunner=runner)
    