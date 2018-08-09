#!/usr/bin/env python

''' 
    Usage:    mkmap --from <datafile> [--to <output_file>] [--maps-only]

'''


import os, sys
import collections
from snap import common
from snap import cli_tools as cli
import jinja2
import docopt
from cmd import Cmd


MAP_CONFIG_TEMPLATE = '''
globals:
  project_home: {{ project_home }}
  lookup_source_module: {{ lookup_module }}
  service_module: {{ service_module }}


sources:
{% for source in datasources %}
  {{source.name}}:
    class: {{source.klass}}
{% endfor %}
maps:{% for map in map_specs %}
  {{ map.name }}:
    settings:
        - name: use_default_identity_transform
          value: True
    lookup_source: 
      {{ map.lookup_source }}
    fields:{% for field_spec in map.fields %}
      - {{ field_spec.target_name }}:
          {% for param in field_spec.params %}{{ param.name }}: {{ param.value }}
          {% endfor %}
    {% endfor %}
{% endfor %}
'''


ParamSpec = collections.namedtuple('ParamSpec', 'name value')
DatasourceSpec = collections.namedtuple('DatasourceSpec', 'name klass')

class FieldSpec(object):
    def __init__(self, name):
        self.target_name = name
        self.params = []

        
    def add_param(self, param_name, param_value):
        self.params.append(ParamSpec(name=param_name, value=param_value))


class MapSpec(object):
    def __init__(self, name, lookup_datasource_name, **kwargs):
        self.name = name
        self.lookup_source = lookup_datasource_name
        self.fields = []


    def add_field(self, field_name, **kwargs):
        field_spec = FieldSpec(field_name)
        for field_param_name, field_param_value in kwargs.items():            
            field_spec.add_param(field_param_name, field_param_value)
        self.fields.append(field_spec)


def create_default_map_from_csv_file(filename, map_name, datasource_name, separator=','):
    header_line = ''
    with open(filename, 'r') as f:
        header_line = f.readline().lstrip().rstrip()

    header_fields = header_line.split(separator)    
    map_spec = MapSpec(map_name,
                       datasource_name,
                       use_default_identity_transform=True)

    for raw_fieldname in header_fields:
        fieldname = raw_fieldname.lstrip().rstrip()
        map_spec.add_field(fieldname, source='record', key=fieldname)
        
    return map_spec


def generate_module_options(module_directory):
    options = []
    for root, dir, files in os.walk(module_directory):
        for filename in [f for f in files if f.endswith('.py')]:
            options.append({'label': filename, 'value': filename.split('.')[0]})
    return options

            
def docopt_cmd(func):
    """
    This decorator is used to simplify the try/except block and pass the result
    of the docopt parsing to the called action.
    """
    def fn(self, arg):
        try:
            opt = docopt_func(fn.__doc__, arg)

        except DocoptExit as e:
            # The DocoptExit is thrown when the args do not match.
            # We print a message to the user and the usage block.

            print('\nPlease specify one or more valid command parameters.')
            print(e)
            return

        except SystemExit:
            # The SystemExit exception prints the usage for --help
            # We do not need to do the print here.

            return

        return func(self, opt)

    fn.__name__ = func.__name__
    fn.__doc__ = func.__doc__
    fn.__dict__.update(func.__dict__)
    return fn



class MakeMapPrompt(Cmd):
    def __init__(self, app_name='m2', **kwargs):
        kwreader = common.KeywordArgReader(*[])
        kwreader.read(**kwargs)
        self.name = app_name
        Cmd.__init__(self)
        self.prompt = '%s> ' % self.name
        self.datasource_specs = kwreader.get_value('datasource_specs') or []
        self.map_specs = kwreader.get_value('maps') or []
        #self.service_objects = kwreader.get_value('service_objects') or []
        globals = kwreader.get_value('global_params') or []


    def project_home_contains_python_source(self):
        for root, dir, files in os.walk(os.getcwd()):
            for f in files:
                if f.endswith('.py'):
                    return True
        return False
                

    def do_quit(self, cmd_args):
        '''Quits the program.'''
        print('mkmap utility exiting.')
        raise SystemExit

    
    def do_q(self, cmd_args):
        '''Quits the program'''
        self.do_quit(cmd_args)

        
    def do_status(self, cmd_args):
        '''Shows the current status of the map build.'''
        print('placeholder')

    
    def configure_datasource(self, cmd_args):
        '''Configures a new lookup datasource.'''

        if not self.project_home_contains_python_source():
            print('No python source files in project home directory "%s".' % self.globals['project_home'])
            return None

        # TODO: parameterize source dir for modules
        source_module = cli.MenuPrompt('datasource_module', generate_module_options(os.getcwd())).show()
        if source_module:
            source_name = cli.InputPrompt('Datasource name').show()
            if source_name:
                self.datasources[source_name] = source_module


    def configure_service_module():
        pass
        
                

    def do_set(self, cmd_args):
        '''Configure one or more datamapper settings.'''
        setting = cli.MenuPrompt.show()
        if not settting:
            return
        if setting == 'datasource':
            self.configure_datasource(cmd_args)

        elif setting == 'service_module':
            self.configure_services(cmd_args)

        elif setting == 'project_home':
            self.set_project_home(cmd_args)

            

        
        
        
        
def main(args):
    j2env = jinja2.Environment()
    template_mgr = common.JinjaTemplateManager(j2env)
    map_template = j2env.from_string(MAP_CONFIG_TEMPLATE)

    datasource_specs = []
    map_specs = []

    prompt = MakeMapPrompt()
    prompt.prompt = 'm2> '
    prompt.cmdloop('starting mkmap in interactive mode...')
    

    datasource_specs = []
    #datasource_specs.append(DatasourceSpec(name='amc', klass='SVODLookupSource'))
    
    
    source_datafile = args['<datafile>']
    mapspec = create_default_map_from_csv_file(source_datafile, 'foobar', 'amc', '\t')    
    map_specs.append(mapspec)

    
    template_data = {
        'project_home': '$APOLLO_HOME',
        'lookup_source_module': 'apollo_datasources',
        'service_module': 'apollo_services',
        'datasources': datasource_specs,
        'map_specs': map_specs
    }

    
    print(map_template.render(**template_data))
    


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

    
