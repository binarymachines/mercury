#!/usr/bin/env python


import unittest
import context
from mercury import datamap as dmap
#import datastores  # this module is defined in the tests directory
from snap import common
import sys
import os
import logging
import yaml

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner

LOG_ID = 'test_data_ingest'
INGEST_YAML_FILE = 'tests/configfiles/sample_ngst_config.yaml'


# this name must exist in the transform yaml file as a direct child of the 'sources' tag
VALID_MAP_NAME = 'test_map'
INVALID_MAP_NAME = 'bad_map'


class RecordIngest(unittest.TestCase):

    def setUp(self):
        self.local_env = common.LocalEnvironment('MERCURY_HOME')
        self.local_env.init()
        home_dir = self.local_env.get_variable('MERCURY_HOME')

        self.yaml_initfile_path = os.path.join(home_dir, INGEST_YAML_FILE)
        self.log = logging.getLogger(LOG_ID)

        self.good_datasource_name = 'SampleDatasource'
        self.bad_datasource_name = 'BadDatasource'
        self.nonexistent_datasource_name = 'NoDatasource'

        self.builder = dmap.RecordTransformerBuilder(self.yaml_initfile_path,
                                                     map_name=VALID_MAP_NAME)
        self.transformer = self.builder.build()


    def tearDown(self):
        pass

    
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger(LOG_ID).setLevel(logging.DEBUG)

    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
        
    unittest.main(testRunner=runner)