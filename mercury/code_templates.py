#!/usr/bin/env python


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
