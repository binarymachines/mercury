#!/usr/bin/env python

import unittest
import context
from mercury import datamap as dmap
import datasources  # this module is defined in the tests directory
from snap import common
import sys
import os
import logging
import yaml

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner


LOG_ID = 'test_data_mapping'
TRANSFORM_YAML_FILE = 'data/sample_transform.yaml'
SCHEMA_FILE = 'data/sample_schema.yaml'

# this name must exist in the transform yaml file as a direct child of the 'sources' tag
VALID_MAP_NAME = 'test_map'
INVALID_MAP_NAME = 'bad_map'



class RecordTransform(unittest.TestCase):

    def setUp(self):
        self.local_env = common.LocalEnvironment('MERCURY_HOME')
        self.local_env.init()
        home_dir = self.local_env.get_variable('MERCURY_HOME')

        self.yaml_initfile_path = os.path.join(home_dir, TRANSFORM_YAML_FILE)
        self.log = logging.getLogger(LOG_ID)

        self.good_datasource_name = 'SampleDatasource'
        self.bad_datasource_name = 'BadDatasource'
        self.nonexistent_datasource_name = 'NoDatasource'

        self.builder = dmap.RecordTransformerBuilder(self.yaml_initfile_path,
                                                     map_name=VALID_MAP_NAME,
                                                     datasource=self.good_datasource_name)
        self.transformer = self.builder.build()



    def tearDown(self):
        pass


    def test_record_transform_creates_record_with_designated_fields(self):
        designated_fields = self.builder.config['maps'][VALID_MAP_NAME]['fields'].keys()
        source_record = {'NAME': 'foo',
                         'COLOR': 'blue',
                         'SKU': 'foo_sku_242',
                         'ID': 22,
                         'COUNT': 'abc',
                         'PRICE': 5.40 }
        target_record = self.transformer.transform(source_record)
        self.log.debug(target_record)
        self.assertSetEqual(set(designated_fields), self.transformer.target_record_fields)


    def test_record_transform_invokes_correct_lookup_method_on_datasource(self):
        source_record = {'NAME': 'foo',
                         'COLOR': 'blue',
                         'SKU': '123.456.789',
                         'ID': 22}
        target_record = self.transformer.transform(source_record)
        self.assertEqual(target_record.get('widget_composite_id'), 'foo_123.456.789')


    def test_record_transform_throws_exception_on_missing_lookup_method(self):
        source_record = {'NAME': 'foo',
                         'COLOR': 'blue',
                         'SKU': '123.456.789',
                         'ID': 22}

        with self.assertRaises(dmap.NoSuchLookupMethodException) as context:
            tfmr = dmap.RecordTransformerBuilder(self.yaml_initfile_path,
                                                 map_name=INVALID_MAP_NAME).build()

            target_record = tfmr.transform(source_record)



    def test_record_transform_resolves_record_key_using_or_syntax(self):
        # the transform is configured to pull the target field "widget_name" from EITHER of two source record fields:
        # "NAME" or "ALIAS", using the | syntax in the "source" field of the config object.
        source_record = {'ALIAS': 'foo',
                         'COLOR': 'blue',
                         'SKU': '123.456.789',
                         'ID': 22}

        target_record = self.transformer.transform(source_record)
        self.assertEquals(target_record.get('widget_name'), source_record['ALIAS'])


    def test_record_transformer_builder_throws_exception_on_missing_datasource(self):

        with self.assertRaises(dmap.NonexistentDatasourceException) as context:
            tfmr = dmap.RecordTransformerBuilder(self.yaml_initfile_path,
                                                 map_name='missing_datasource_map').build()




if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger(LOG_ID).setLevel(logging.DEBUG)

    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
        
    unittest.main(testRunner=runner)
    
    