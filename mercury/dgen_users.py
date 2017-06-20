#!/usr/bin/env python

'''Usage: dgen_users.py <initfile> 
'''


import docopt
import os, sys
import tdxutils as tdx
from snap import common, snap
from snap import csvutils as csvu
from  datetime import datetime



users_query = '''
SELECT id, country, created_at 
FROM users
'''




def main(args):

    init_filename = args['<initfile>']
    s3_bucket_name = 'labs.dexter'   #args.get('<s3_bucket>')

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    yaml_config = common.read_config_file(init_filename)
    service_objects =  snap.initialize_services(yaml_config, tdx.logger)
    service_object_registry = common.ServiceObjectRegistry(service_objects)    
    redshift_svc = service_object_registry.lookup('redshift')
    password = os.getenv('REDSHIFT_READONLY_PASSWORD')
    redshift_svc.login(password)

    user_records = []
    with redshift_svc.get_connection() as conn:
        rsec = tdx.RedshiftS3Context(s3_bucket_name, aws_access_key_id, aws_secret_key)
        exporter = tdx.RedshiftS3Exporter(conn)
        s3paths = exporter.export_records_as_csv(users_query, 'tdx/dim_tables/users_', rsec)

        print s3paths
        print rsec.generate_copy_statement('d_users', s3paths['manifest'])

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
    
    
