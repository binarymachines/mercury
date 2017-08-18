#!/usr/bin/env python
import context
from lettuce import world
from mercury import telegraf as tg
from mercury import sqldbx as sqlx
from snap import common
import os, sys
import yaml
import logging
import logging.handlers
from snap import testutils as tutils
from sqlalchemy import Integer, String, DateTime, Float, Boolean, text, and_, bindparam
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

env_variable_list = {}
local_env = common.LocalEnvironment('LETTUCE_TEST_DATA_DIR',
                                    'PIPELINE_CONFIG_FILE',
                                    'PGSQL_HOST',
                                    'PGSQL_USERNAME',
                                    'PGSQL_PASSWORD',
                                    'MX_HOME',
                                    'OLAP_MAPPING_CONFIG_FILE',
                                    'CONFIG_DIR')
local_env.init()

world.data_dir = local_env.get_variable('LETTUCE_TEST_DATA_DIR')
world.config_dir = local_env.get_variable('CONFIG_DIR')
pipeline_cfg_file_path = os.path.join(world.data_dir, local_env.get_variable('PIPELINE_CONFIG_FILE'))
world.mapping_schema_filename = os.path.join(world.config_dir, local_env.get_variable('OLAP_MAPPING_CONFIG_FILE'))

world.pgsql_host = local_env.get_variable('PGSQL_HOST')
world.pgsql_username = local_env.get_variable('PGSQL_USERNAME')
world.pgsql_password = local_env.get_variable('PGSQL_PASSWORD')
world.prx_home = local_env.get_variable('PRX_HOME')


yaml_config = None
with open(pipeline_cfg_file_path) as f:
    yaml_config = yaml.load(f)
    world.pipeline_config = tg.KafkaPipelineConfig(yaml_config)

world.test_environment = tutils.TestEnvironment()

logfile_name = yaml_config['globals']['logfile']
handler = logging.handlers.RotatingFileHandler(logfile_name, maxBytes=10000, backupCount=1)
log = logging.getLogger('integration_tests')
logging.basicConfig(filename=logfile_name, level=logging.DEBUG)
log.addHandler(handler)

world.log = log

olap_test_context = tutils.TestContext('etl_raw_records_to_olap',
                                       test_file_alias='my_datafile',
                                       loaded_raw_record_list=[],
                                       promise_queue_errors=[],
                                       consumed_raw_into_sst_record_list=[],
                                       offset=0, loaded_sst_record_list=[],
                                       consumed_sst_into_olap_record_list=[],
                                       log_id='olap_consume_steps')

send_receive_test_context = tutils.TestContext('extract_raw_records_to_staging',
                                               test_file_alias='my_datafile',
                                               loaded_raw_record_list=[],
                                               promise_queue_errors=[],
                                               consumed_raw_record_list=[], offset=0,
                                               log_id='send_receive_log',
                                               num_received_records=0)

loaded_data_commits_context = tutils.TestContext('load_data_with_checkpoints',
                                                 test_file_alias='my_datafile',
                                                 offset=0, log_id='load_data_checkpoint_log', num_checkpoint_errors=0,
                                                 num_successful_checkpoints=0, num_received_records=0,
                                                 checkpoint_frequency=0)


world.test_environment.register_context('etl_raw_records_to_olap', olap_test_context)
world.test_environment.register_context('extract_raw_records_to_staging', send_receive_test_context)
world.test_environment.register_context('load_data_with_checkpoints',
                                        loaded_data_commits_context)