#!/usr/bin/env python

'''Usage:  
         notify_procgen -i <initfile> -c <event_channel> (--trigger | --proc)
         notify_procgen -i <initfile> channels

   Options:
         -c, --channel    channel name
         -i, --initfile   initfile name
'''

import docopt
import jinja2
import eavesdroppr
from snap import common


JSON_BUILD_FUNC_TEMPLATE = '''
json_build_object('table', TG_TABLE_NAME,
                  {% for field in payload_fields %}'{{field}}', {{field}},
                  {% endfor %}'type', TG_OP)
'''

PROC_TEMPLATE = '''
CREATE OR REPLACE FUNCTION {schema}.table_update_notify() RETURNS trigger AS $$
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


def main(args):
    yaml_config = common.read_config_file(args['<initfile>'])
    if args['--channel'] == False:
        print('\n'.join(yaml_config['channels'].keys()))
        return 0

    event_channel = args['<event_channel>']
    if not yaml_config['channels'].get(event_channel):
        raise eavesdroppr.NoSuchEventChannel(event_channel)

    channel_config = yaml_config['channels'][event_channel]
    
    operation = channel_config['db_operation']
    if not operation in SUPPORTED_DB_OPS:
        raise eavesdroppr.UnsupportedDBOperation(operation)

    table_name = channel_config['db_table_name']
    db_schema = channel_config.get('db_schema') or 'public'
    proc_name = channel_config.get('db_proc_name') or '%s_%s_notify' % (table_name, operation.lower())
    trigger_name = channel_config.get('db_trigger_name') or 'trg_%s_%s' % (table_name, operation.lower())
    source_fields = channel_config['payload_fields']
    
    j2env = jinja2.Environment()
    template_mgr = common.JinjaTemplateManager(j2env)        
    json_func_template = j2env.from_string(JSON_BUILD_FUNC_TEMPLATE)
    json_func = json_func_template.render(payload_fields=source_fields)
    
    pk_field = channel_config['pk_field_name']
    pk_type = channel_config['pk_field_type']

    if args['--proc']:
        print(PROC_TEMPLATE.format(schema=db_schema,
                                   pk_field_name=pk_field,
                                   pk_field_type=pk_type,
                                   channel_name=event_channel,
                                   json_build_func=json_func))

    elif args['--trigger']:
        print(TRIGGER_TEMPLATE.format(schema=db_schema,
                                      table_name=table_name,
                                      trigger_name=trigger_name,
                                      db_proc_name=proc_name,
                                      db_op=operation))

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
