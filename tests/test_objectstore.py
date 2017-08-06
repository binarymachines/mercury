#!/usr/bin/env python

import unittest
import context

from snap import common
import sys
import logging
import yaml

from mercury import sqldbx as sqlx
from mercury import datamap as dmap
from mercury import objectstore as obs

LOG_ID = 'test_objectstore'
TESTING_SCHEMA = 'test'
OBJECT_TABLE_NAME = 'objects'

SAMPLE_DATA = '''
TYPE|UUID|PRICE|DATE
"sale"|"abc"|1.00|01-01-2016
"sale"|"def"|2.25|01-02-2016
"sale"|"ghi"|1.53|01-15-2016
'''


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
                                            fact_id_field='event_uuid',
                                            pk_default='gen_random_uuid()')

        self.tablespec_builder.add_data_field('event_type', 'varchar(32)')
        self.tablespec_builder.add_data_field('event_uuid', 'varchar(32)')
        self.tablespec_builder.add_data_field('price', 'float')
        self.tablespec_builder.add_data_field('event_date', 'date')
        self.tablespec_builder.add_meta_field('generation', 'int4')
        self.tablespec_builder.add_meta_field('correction_id', 'int4')

        #self.create_object_table(self.objectstore_db, tablespec_builder.build())

        
    def tearDown(self):        
        pass
        #self.destroy_object_table(self.objectstore_db)


    def load_test_data(self, db, tablespec):
        pass


    def create_object_table(self, db, tablespec):
        with sqlx.txn_scope(db) as session:
            session.execute(tablespec.sql)


    def destroy_object_table(self, db):
        with sqlx.txn_scope(db) as session:
            drop_statement = 'DROP TABLE "%s"."%s"' % (TESTING_SCHEMA, OBJECT_TABLE_NAME)
            session.execute(drop_statement)


    def test_tablespec_generates_correct_sql(self):
        tablespec = self.tablespec_builder.build()
        self.log.debug(tablespec.sql)
        self.assertTrue(False)


    def test_accumulator_generates_correct_loading_query(self):
        tablespec = self.tablespec_builder.build()

        self.create_object_table(self.objectstore_db, tablespec)
        self.load_test_data(self.objectstore_db, tablespec)

        acc = obs.InMemoryAccumulator(tablespec, self.objectstore_db)
        loading_query = acc.generate_load_query(0)
        self.log.debug(loading_query)

        acc.load_source_data(max_generation=0)
        self.log.debug(acc.get_data())

        self.assertTrue(False)


    def test_in_memory_accumulator_generates_correct_dataset(self):
        self.assertTrue(False)



if __name__ == '__main__':
    logging.basicConfig( stream=sys.stderr )
    logging.getLogger(LOG_ID).setLevel( logging.DEBUG )
    unittest.main()
    