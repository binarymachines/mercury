#!/usr/bin/env python

'''
Usage:        test_record_validation (--configfile=<config_filename>) (--type=<record_type>) <input_file>

'''

import docopt
import re
import mx_utils as mx
import yaml
import csv
from snap import common



class FilteredRecordOutputChannel(object):
    def __init__(self, **kwargs):
        self._records_sent = 0
        self._abort_on_fail = kwargs.get('abort_on_fail', False)


    def _send(self, record):
        '''implement in subclass
        '''
        pass


    def send(self, record_dict):
        try:
            self._send(record_dict)
            self._records_sent += 1
        except Exception, err:
            if self._abort_on_fail:
                self.__del__()
                raise err # TODO: incorporate error tracking/reporting


class FileOutputChannel(FilteredRecordOutputChannel):
    def __init__(self, **kwargs):
        FilteredRecordOutputChannel.__init__(self, kwargs)
        filename = kwargs.get('filename')
        if not filename:
            raise Exception('missing keyword argument: "filename"')

        mode_string = kwargs.get('mode', '')
        self._output_file = open(filename, mode_string)


    def _send(self, record):
        self._output_file.write(str(record))

    def __del__(self):
        self._output_file.close()



class ConsoleOutputChannel(FilteredRecordOutputChannel):
    def __init__(self, **kwargs):
        FilteredRecordOutputChannel.__init__(self, kwargs)


    def _send(self, record):
        print common.jsonpretty(record)



def main(args):
    config_filename = args['--configfile']

    yaml_config = None
    with open(config_filename, 'r') as f:
        yaml_config = yaml.load(f)

    record_type = args['--type']
    schema_config = yaml_config['record_types'].get(record_type)
    if not schema_config:
        raise Exception('no record type "%s" specified in config file' % record_type)

    validator = mx.TextRecordValidationProfile(record_type, schema_config)
    input_filename = args['<input_file>']
    with open(input_filename, 'rb') as datafile:
        csv_reader = csv.DictReader(datafile, delimiter='|', quotechar='"')
        for raw_record in csv_reader:

            clean_record = {}
            for key in raw_record.keys():
                clean_record[key] = raw_record.get(key).strip()
            print common.jsonpretty(clean_record)
            status = validator.check_record(clean_record)
            print status


if __name__=='__main__':
    args = docopt.docopt(__doc__)
    main(args)
