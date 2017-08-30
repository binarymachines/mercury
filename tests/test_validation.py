import context
import unittest
import yaml
from mercury import mx_utils as mx

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner


class RecordValidationTest(unittest.TestCase):
    def setUp(self):
        config_filename = 'data/sample_schema.yaml'

        yaml_config = None
        with open(config_filename, 'r') as f:
            yaml_config = yaml.load(f)

        record_type = 'widget_record'
        schema_config = yaml_config['record_types'].get(record_type)
        if not schema_config:
            raise Exception('no record type "%s" specified in config file' % record_type)

        self.validator = mx.TextRecordValidationProfile(record_type, schema_config)


    def tearDown(self):
        pass


    def test_record_with_schema_error_should_fail(self):
        raw_record = ({"NAME": "sample_widget", "SKU": "skuvalue", "ID": "some_id"})
        status = self.validator.check_record(raw_record)
        self.assertFalse(status.is_ok())


    def test_record_with_n_errors_should_fail_with_n_errcodes(self):
        raw_record = ({"NAME": "sample_widget", "ID": "some_id"})
        status = self.validator.check_record(raw_record)
        self.assertEquals(len(status.get_errors()), 2)


    def test_record_with_no_errors_should_pass(self):
        input_record = {'NAME': 'foo',
                        'COLOR': 'blue',
                        'SKU': 'foo_sku_242',
                        'ID': '22' }
        status = self.validator.check_record(input_record)
        self.assertTrue(status.is_ok())


    def test_record_with_datatype_error_should_fail(self):
        input_record = {'NAME': 'foo',
                        'COLOR': 'blue',
                        'SKU': 'foo_sku_242',
                        'ID': 22,
                        'COUNT': 'abc',
                        'PRICE': 5.40 }
        status = self.validator.check_record(input_record)
        self.assertFalse(status.is_ok())



if __name__ == '__main__':
    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
        
    unittest.main(testRunner=runner)
