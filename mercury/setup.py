#!/usr/bin/env python

'''Usage: setup.py
'''

import docopt
import os
import subprocess
from jinja2 import Template
import logging
import time
import tdxutils as tdx
import ansiblex as anx
from snap import common


LOG_TAG = 'METL_setup'


cluster_init_cmd_template = '''
{{couchbase_home}}/bin/couchbase-cli cluster-init -u Administrator -p {{admin_password}} --cluster={{host}} --cluster-username=Administrator --cluster-password={{admin_password}} --cluster-ramsize={{ram_size}} --cluster-index-ramsize={{index_ram_size}} --services={{services}}
'''

bucket_create_cmd_template = '''
{{couchbase_home}}/bin/couchbase-cli bucket-create -c {{host}}:{{port}} -u Administrator -p {{admin_password}} --bucket={{bucket_name}} --bucket-ramsize={{ram_size}} --bucket-type={{type}} --bucket-priority=low --enable-flush=1
'''

index_create_cmd_template = """
'CREATE PRIMARY INDEX ON `{{bucket_name}}` USING GSI'
"""

secondary_index_create_commands = """
cbc n1ql 'CREATE INDEX status_idx ON `tdx_data`(status) USING GSI';
cbc n1ql 'CREATE INDEX rectype_idx ON `tdx_data`(record_type) USING GSI';
cbc n1ql 'CREATE INDEX orderid_idx ON `tdx_data`(source_record.order_id) USING GSI';
cbc n1ql 'CREATE INDEX status_processed_idx ON `tdx_data`(status) WHERE status = "processed" USING GSI';
"""


cluster_config = {
    'couchbase_home': os.getenv('COUCHBASE_HOME'),
    'host': 'localhost',
    'ram_size': 3900,
    'index_ram_size': 1024, 
    'admin_password': os.getenv('COUCHBASE_ADMIN_PASSWORD'),
    'services': 'index,data,query'
}

bucket_config_tbl = {
    'tdx_data': { 'ram_size': 2000, 'type': 'couchbase'  },
    'tdx_journal': { 'ram_size': 512, 'type': 'couchbase' },
    'tdx_cache': { 'ram_size': 1024, 'type': 'memcached' },
    'default': { 'ram_size': 300, 'type': 'couchbase' }
}


buckets = ['default', 'tdx_data', 'tdx_journal', 'tdx_cache']


def generate_cluster_init_cmd(command_template, data):
    j2template = Template(command_template)
    return j2template.render(data).strip()


def generate_bucket_create_cmd(bucket_name, command_template, config_data, cluster_config_data):

    config_data['bucket_name'] = bucket_name
    config_data['couchbase_home'] = cluster_config_data['couchbase_home']
    config_data['host'] = cluster_config_data['host']
    config_data['port'] = cluster_config_data.get('port', 8091)
    config_data['admin_password'] = cluster_config_data['admin_password']
    j2template = Template(command_template)
    return j2template.render(config_data).strip()
    

def generate_index_cmd(bucket_name, command_template):
    j2template = Template(command_template)
    return j2template.render({'bucket_name': bucket_name})



def update_crontab():
    tdx_home = os.getenv('TDX_HOME')
    if not tdx_home:
        raise common.MissingEnvironmentVarException('TDX_HOME')
    launcher_path = os.path.join(tdx_home, 'tdx_launcher.sh')
    cron_params = dict(name="run ETL pipeline",
                       minute=0,
                       hour=0,
                       job=launcher_path)
    
    psb = anx.PlaySourceBuilder().add_task('cron', cron_params)
    ansible_context = anx.AnsibleContext()
    ansible_context.execute_play(psb.build())


def create_indices(bucket_names, max_tries):
    log = logging.getLogger(LOG_TAG)
    indices_created = False
    index_retry_wait_time_seconds = 5
        
    for bucket_name in bucket_names:
        num_tries = 1
        n1ql_stmt = 'CREATE PRIMARY INDEX ON `%s` USING GSI' % bucket_name
        index_create_cmd = 'cbc n1ql %s' % n1ql_stmt

        while num_tries <= max_tries:
            log.info('### Executing index create command: %s\n' % index_create_cmd)
            try:
                subprocess.check_output(['cbc', 'n1ql', n1ql_stmt], stderr=subprocess.STDOUT)
                log.info('primary index on bucket %s created.' % bucket_name)
                break
            except subprocess.CalledProcessError, err:
                log.error('index creation command failed with error: %s' % err.output)
                if num_tries == max_tries:
                    log.error('unable to create index, giving up.')
                else:
                    log.error('will retry in %d seconds...' % index_retry_wait_time_seconds)
                    time.sleep(index_retry_wait_time_seconds)
                    log.error('retrying...')
            except Exception, e:
                log.error('catching an exception other than CalledProcessError.')
                log.error(str(e))
                time.sleep(index_retry_wait_time_seconds)
                log.error('retrying...')
                
            finally:
                num_tries += 1
    
    
    

def main(args):
    log = tdx.init_logging(LOG_TAG, 'tdx_setup.log', logging.DEBUG)
    log.info('setup script starting.')

    log.info('updating cron to run TDX launcher daily...')
    update_crontab()
    
    retry_wait_time_seconds = 2
    max_tries = 5
    cluster_init_cmd = generate_cluster_init_cmd(cluster_init_cmd_template, cluster_config)

    log.info('### Executing cluster init command: %s\n' % cluster_init_cmd)
    cluster_is_up = False
    num_tries = 1
    while num_tries <= max_tries and not cluster_is_up:
        try:
            subprocess.check_output(cluster_init_cmd.split(' '), stderr=subprocess.STDOUT)
            cluster_is_up = True
        except subprocess.CalledProcessError, err:            
            log.error('cluster init command failed with error: %s' % err.output)
            if '[Errno 111]' in err.output: # error 111 is "connection refused"
                if num_tries == max_tries:
                    log.error('couchbase still nonresponsive after %d tries, giving up.' % num_tries)
                else:
                    log.error('will retry in %d seconds...' % retry_wait_time_seconds)
                    time.sleep(retry_wait_time_seconds)                    
            else:
                log.error('catching an error from couchbase-cli other than connection refused:')
                log.error(err)
                break
        except Exception, e:
            log.error('catching an exception other than CalledProcessError.')
            log.error(str(e))
            break
        
        finally:
            num_tries += 1

    
    if not cluster_is_up:
        log.error('unable to initialize cluster.')
        

    # we will still try to create the buckets, in case we are running this script
    # against a couchbase instance with a live cluster but no buckets.
    #
    for bucket_name, bucket_config in bucket_config_tbl.iteritems():
        bucket_cmd = generate_bucket_create_cmd(bucket_name, bucket_create_cmd_template, bucket_config, cluster_config)            
        log.info('### Executing bucket create command: %s\n' % bucket_cmd)
        try:
            subprocess.check_output(bucket_cmd.split(' '), stderr=subprocess.STDOUT)                
        except subprocess.CalledProcessError, err:
            log.error('bucket creation command failed with error: %s' % err.output)


    create_indices(['tdx_journal'], max_tries)

                
                
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
    
