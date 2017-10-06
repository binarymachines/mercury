#!/usr/bin/env python

'''Usage: eavesdrop
'''




import docopt
import jinja2
from snap import common


JSON_BUILD_FUNC_TEMPLATE = '''
json_build_object('table', TG_TABLE_NAME,
                  {% for field in payload_fields %}'{{field}}', {{field}},
                  {% endfor %}
                  'type', TG_OP)
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

def main(args):
    #print args

    source_fields = ['id']

    j2env = jinja2.Environment()
    template_mgr = common.JinjaTemplateManager(j2env)        
    json_func_template = j2env.from_string(JSON_BUILD_FUNC_TEMPLATE)
    json_func = json_func_template.render(payload_fields=source_fields)

    print PROC_TEMPLATE.format(schema='egress',
                               pk_field_name='id',
                               pk_field_type='bigint',
                               channel_name='ch_table_insert',
                               json_build_func=json_func)

    
    


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
