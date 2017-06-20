#!/usr/bin/env python

'''Usage: dgen_brands.py <initfile>
'''


import docopt
import os, sys
import tdxutils as tdx
from snap import common, snap
from snap import csvutils as csvu

brands_query = '''
SELECT id, name, subdomain, country, state, language, currency, created_at
FROM stores
'''


DELIMITER = '\t'


def main(args):

    init_filename = args['<initfile>']

    s3_bucket_name = 'labs.dexter' # TODO: get this from the command line
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    yaml_config = common.read_config_file(init_filename)
    service_objects =  snap.initialize_services(yaml_config, tdx.logger)
    service_object_registry = common.ServiceObjectRegistry(service_objects)    
    redshift_svc = service_object_registry.lookup('redshift')
    password = os.getenv('REDSHIFT_READONLY_PASSWORD')
    redshift_svc.login(password)

    with redshift_svc.get_connection() as conn:
        rsec = tdx.RedshiftS3Context(s3_bucket_name, aws_access_key_id, aws_secret_key)
        exporter = tdx.RedshiftS3Exporter(conn)
        s3_paths = exporter.export_records_as_csv(brands_query, 'tdx/dim_tables/brands_', rsec, None, delimiter=DELIMITER)

        print s3_paths
        print rsec.generate_copy_statement('d_brands', s3_paths['manifest'], options_string="manifest delimiter '%s'" % DELIMITER)
        

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
    
    
