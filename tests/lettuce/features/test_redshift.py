#!/usr/bin/env python


import context
import sys, os, unittest, json
import logging
from snap import common
from mercury import sqldbx

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner


LOG_ID = 'test_redshift'
AUTH_FILE = '~/Project'


class DatabaseConnectionTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def createRedshiftDB(self):
        filepath = os.path.join(os.environ['DB_API_AUTH'], 'teachable-db-auth.json')
        with open(os.path.expanduser(filepath), 'r') as f_auth:
            auths = json.load(f_auth)

        self.db_username = auths['redshift']['user']
        self.db_password = auths['redshift']['password']
        self.db_name = auths['redshift']['database']
        host = auths['redshift']['host']
        port = int(auths['redshift']['port'])
        return sqldbx.RedshiftDatabase(host, self.db_name, port)


    def test_redshift_client_can_connect(self):
        db = self.createRedshiftDB()
        db.login(self.db_username, self.db_password, self.db_name)
        pmgr = sqldbx.PersistenceManager(db)
        session = pmgr.get_session()

        self.assertIsNotNone(session, "Session object is None")


    def test_redshift_client_can_run_simple_select_query(self):
        db = self.createRedshiftDB()
        db.login(self.db_username, self.db_password, self.db_name)
        pmgr = sqldbx.PersistenceManager(db)
        session = pmgr.get_session()
        dbconnection = pmgr.database.engine.connect()

        stmt = "SELECT 1 as ping;"

        result = dbconnection.execute(stmt)
        print('Query Results: \n\t', result.fetchall())
        self.assertIsNotNone(result, "query result is None")


    def test_redshift_client_can_run_filter_type_query(self):
        # Not Going To Push Data To Test This Just Yet
        self.assertTrue(True)



if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger(LOG_ID).setLevel(logging.DEBUG)

    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()

    unittest.main(testRunner=runner)
