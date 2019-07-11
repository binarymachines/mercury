#!/usr/bin/env python


import os, sys
import pgpubsub
from snap import snap, common
from snap import cli_tools as cli

from mercury import configtemplates as config
#from mercury import metaobjects as meta
import logging
import jinja2
import json
from cmd import Cmd
from docopt import docopt as docopt_func
from docopt import DocoptExit

JSON_BUILD_FUNC_TEMPLATE = '''
json_build_object('table', TG_TABLE_NAME,
                  'primary_key', {{pk_field}},
                  {% for field in payload_fields %}'{{field}}', NEW.{{field}},
                  {% endfor %}'type', TG_OP)
'''

PROC_TEMPLATE = '''
CREATE OR REPLACE FUNCTION {schema}.{proc_name}() RETURNS trigger AS $$
DECLARE
  {pk_field_name} {pk_field_type};
BEGIN
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    {pk_field_name} = NEW.{pk_field_name};
  ELSE
    {pk_field_name} = OLD.{pk_field_name};
  END IF;
  PERFORM pg_notify('{channel_name}',
                    {json_build_func}::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
'''


TRIGGER_TEMPLATE = '''
DROP TRIGGER IF EXISTS {trigger_name} ON {schema}.{table_name};
CREATE TRIGGER {trigger_name} AFTER {db_op} ON {schema}.{table_name} 
FOR EACH ROW 
EXECUTE PROCEDURE {schema}.{db_proc_name}();
'''


SUPPORTED_DB_OPS = ['INSERT', 'UPDATE']

OPERATION_OPTIONS = [{'value': 'INSERT', 'label': 'INSERT'}, {'value': 'UPDATE', 'label': 'UPDATE'}]


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('eavesdroppr')


class NoSuchEventChannel(Exception):
    def __init__(self, channel_id):
        Exception.__init__(self,
                           'No event channel registered under the name "%s". Please check your initfile.' \
                           % channel_id)


class NoSuchEventHandler(Exception):
    def __init__(self, handler_func_name, handler_module):
        Exception.__init__(self,
                           'No event handler function "%s" exists in event handler module "%s".' \
                           % (handler_func_name, handler_module))


class UnsupportedDBOperation(Exception):
    def __init__(self, operation):
        Exception.__init__(self, 'The database operation "%s" is not supported.' % operation)


def default_proc_name(table_name, operation):
    return '%s_%s_notify' % (table_name, operation.lower())


def default_trigger_name(table_name, operation):
    return 'trg_%s_%s' % (table_name, operation.lower())


def generate_code(event_channel, channel_config, **kwargs):
    operation = channel_config['db_operation']
    if not operation in SUPPORTED_DB_OPS:
        raise UnsupportedDBOperation(operation)

    table_name = channel_config['db_table_name']
    db_schema = channel_config.get('db_schema') or 'public'
    procedure_name = channel_config.get('db_proc_name') or default_proc_name(table_name, operation)
    trigger_name = channel_config.get('db_trigger_name') or default_trigger_name(table_name,
                                                                                 operation)
    source_fields = channel_config['payload_fields']
    primary_key_field = channel_config['pk_field_name']
    primary_key_type = channel_config['pk_field_type']

    j2env = jinja2.Environment()
    template_mgr = common.JinjaTemplateManager(j2env)
    json_func_template = j2env.from_string(JSON_BUILD_FUNC_TEMPLATE)
    json_func = json_func_template.render(payload_fields=source_fields,
                                          pk_field=primary_key_field)

    if kwargs['procedure']:
        print(PROC_TEMPLATE.format(schema=db_schema,
                                        proc_name=procedure_name,
                                        pk_field_name=primary_key_field,
                                        pk_field_type=primary_key_type,
                                        channel_name=event_channel,
                                        json_build_func=json_func))

    elif kwargs['trigger']:
        print(TRIGGER_TEMPLATE.format(schema=db_schema,
                                      table_name=table_name,
                                      trigger_name=trigger_name,
                                      db_proc_name=procedure_name,
                                      db_op=operation))


def default_event_handler(event, svc_registry):
    print(common.jsonpretty(json.loads(event.payload)))


def listen(channel_id, handler_func, pubsub_connector_func, svc_registry, **kwargs):
    #local_env = common.LocalEnvironment('PGSQL_USER', 'PGSQL_PASSWORD')
    #local_env.init()

    #pgsql_user = local_env.get_variable('PGSQL_USER')
    #pgsql_password = local_env.get_variable('PGSQL_PASSWORD')
    #db_host = yaml_config['globals']['database_host']
    #db_name = yaml_config['globals']['database_name']

    '''
    pubsub = pgpubsub.connect(host=db_host,
                              user=pgsql_user,
                              password=pgsql_password,
                              database=db_name)
    '''

    '''
    pubsub = pubsub_connector.init()                              
    handler_module_name = yaml_config['globals']['handler_module']

    project_dir = common.load_config_var(yaml_config['globals']['project_directory'])
    sys.path.append(project_dir)
    handlers = __import__(handler_module_name)
    handler_function_name = yaml_config['channels'][channel_id].get('handler_function') or 'default_handler'

    if handler_function_name != 'default_handler':
        if not hasattr(handlers, handler_function_name):
            raise NoSuchEventHandler(handler_function_name, handler_module_name)

        handler_function = getattr(handlers, handler_function_name)
    else:
        handler_function = default_event_handler

    service_objects = common.ServiceObjectRegistry(snap.initialize_services(yaml_config))
    '''

    pubsub = pubsub_connector_func(svc_registry)
    pubsub.listen(channel_id)
    print('### listening on channel "%s"...' % channel_id, file=sys.stderr)
    for event in pubsub.events():
        handler_func(event, svc_registry)

