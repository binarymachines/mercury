#!/usr/bin/env python


import context
import os, unittest
import logging
from snap import common
from mercury import sqldbx

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner


LOG_ID = 'test_dbservices'


class DatabaseConnectionTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def createPostgresDB(self):
        self.db_username = 'praxisdba'
        self.db_password = common.load_config_var('$POSTGRESQL_PASSWORD')
        self.db_name = 'scratch'
        host = 'localhost'
        port = 5432
        return sqldbx.PostgreSQLDatabase(host, self.db_name, port)

    def test_mysql_client_can_connect(self):
        self.assertTrue(False)


    def test_mssql_client_can_connect(self):
        self.assertTrue(False)


    def test_postgres_client_can_connect(self):
        db = self.createPostgresDB()
        db.login(self.db_username, self.db_password, self.db_name)
        pmgr = sqldbx.PersistenceManager(db)
        session = pmgr.getSession()

        self.assertIsNotNone(session, "Session object is None")


    def test_postgres_client_can_run_simple_select_query(self):
        db = self.createPostgresDB()
        db.login(self.db_username, self.db_password, self.db_name)
        pmgr = sqldbx.PersistenceManager(db)
        session = pmgr.getSession()
        dbconnection = pmgr.database.engine.connect()

        stmt = "SELECT 'Hello'"

        result = dbconnection.execute(stmt)
        self.assertIsNotNone(result, "query result is None")


    def test_postgres_client_can_run_filter_type_query(self):
        self.assertTrue(False)


        
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger(LOG_ID).setLevel(logging.DEBUG)

    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
        
    unittest.main(testRunner=runner)
    
