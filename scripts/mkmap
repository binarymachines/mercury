#!/usr/bin/env python

''' 
    Usage:    mkmap
              mkmap --from <datafile> [--to <output_file>]

'''


import os, sys
import collections
from snap import common
from snap import cli_tools as cli
import jinja2
import docopt
from docopt import docopt as docopt_func
from docopt import DocoptExit
from cmd import Cmd


MAP_CONFIG_TEMPLATE = '''
globals:
  project_home: {{ project_home }}
  datasource_module: {{ datasource_module }}
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
      {{ map.datasource }}

    fields:{% for field_spec in map.fields %}
      - {{ field_spec.target_name }}:
          {% for param in field_spec.params %}{{ param.name }}: {{ param.value }}
          {% endfor %}
    {% endfor %}
{% endfor %}
'''

ParamSpec = collections.namedtuple('ParamSpec', 'name value')
DatasourceSpec = collections.namedtuple('DatasourceSpec', 'name klass')


def named_tuple_array_to_dict(tuple_array, **kwargs):
    kwreader = common.KeywordArgReader('key_name', 'value_name')
    kwreader.read(**kwargs)
    data = {}
    key_name = kwreader.get_value('key_name')
    value_name = kwreader.get_value('value_name')
    
    for named_tuple in tuple_array:
        key = getattr(named_tuple, key_name)
        value = getattr(named_tuple, value_name)
        data[key] = value
    return data
        

class FieldSpec(object):
    def __init__(self, name):
        self.target_name = name
        self.params = []

        
    def add_param(self, param_name, param_value):
        self.params.append(ParamSpec(name=param_name, value=param_value))


class MapSpec(object):
    def __init__(self, name, datasource_name, **kwargs):
        self.name = name
        self.datasource = datasource_name
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

    separator = separator.encode().decode('unicode_escape')
    if not separator in header_line:
        raise Exception('separator "%s" not found in header line!!!' % separator)
    header_fields = header_line.split(separator)    
    map_spec = MapSpec(map_name,
                       datasource_name,
                       use_default_identity_transform=True)

    print('### %d header fields found.' % len(header_fields))

    for raw_fieldname in header_fields:
        fieldname = raw_fieldname.lstrip().rstrip()
        map_spec.add_field(fieldname, source='record', key=fieldname)
        
    return map_spec


def generate_module_options_from_directory(module_directory):
    options = []
    for root, dir, files in os.walk(module_directory):
        for filename in [f for f in files if f.endswith('.py')]:
            options.append({'label': filename, 'value': filename.split('.')[0]})
    return options


def generate_class_options_from_module(module_name):
    options = []
    module = __import__(module_name)
    module_dict = module.__dict__
    for key, value in module_dict.items():
        if value.__class__.__name__ == 'type':
            options.append({'label': key, 'value': key})

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


GLOBAL_OPTIONS = [{'value': 'project_home', 'label': 'Project Home'},
                  {'value': 'datasource_module', 'label': 'Datasource Module'},
                  {'value': 'service_module', 'label': 'Service Module'}]

UPDATE_OPTIONS = [{'value': 'global_settings', 'label': 'Global settings'},
                  {'value': 'map', 'label': 'Existing data map'}]


class MakeMapCLI(Cmd):
    def __init__(self, app_name='mkmap', **kwargs):
        kwreader = common.KeywordArgReader(*[])
        kwreader.read(**kwargs)
        self.name = app_name
        Cmd.__init__(self)
        self.prompt = '%s> ' % self.name
        self.datasource_specs = kwreader.get_value('datasource_specs') or []
        self.map_specs = kwreader.get_value('maps') or []
        self.service_objects = kwreader.get_value('service_objects') or []
        self.globals = []
        self.globals.append(ParamSpec(name='project_home', value=''))
        self.globals.append(ParamSpec(name='datasource_module', value=''))
        self.globals.append(ParamSpec(name='service_module', value=''))

        self.initial_datafile = kwargs.get('initial_sourcefile')
        _ = os.system('clear')


    @property
    def project_home(self):
        return common.load_config_var(self.get_current_project_setting('project_home'))


    @property
    def datasource_module(self):
        return self.get_current_project_setting('datasource_module')


    def generate_datasource_options(self):
        options = []
        for source in self.datasource_specs:
            options.append({'value': source.name, 'label': source.name})
        return options


    def project_home_contains_python_source(self):
        for root, dir, files in os.walk(self.project_home):
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


    def configure_map(self, map_name):
        print('placeholder')
        
    
    def configure_datasource(self, cmd_args):
        '''Configures a new lookup datasource.'''

        if not self.project_home_contains_python_source():
            print('No python source files in project home directory "%s".' % self.project_home)
            return None

        if not self.datasource_module:
            print('The lookup datasource module has not been configured for this project.')
            should_set = cli.InputPrompt('Set it now (Y/n)?', 'y').show()
            if should_set == 'y':
                self.do_globals({'update': True})
                if not self.datasource_module:
                    print('project datasource module not updated.')
                    return

        source_module_name = self.datasource_module
        print('scanning python module %s for datasources...' % source_module_name)
        
        class_options = generate_class_options_from_module(source_module_name)
        if not len(class_options):
            print('\nThe python module "%s" contains no eligible types. Please check your code.\n' % source_module_name)
            return
  
        class_name = cli.MenuPrompt('Datasource name', class_options).show()            
        if not class_name:
            return

        print('\nUsing datasource class %s from module %s.\n' % (class_name, source_module_name))
            
        datasource_label = cli.InputPrompt('Enter an alias for this datasource').show()
        if not datasource_label:
            return

        should_create = cli.InputPrompt('Register datasource "%s" using class %s (Y/n)?'
                                        % (datasource_label, class_name), 'y').show()
        if should_create == 'y':            
            return DatasourceSpec(name=datasource_label, klass=class_name)
                

    def configure_service_module():
        pass


    @docopt_cmd
    def do_list(self, cmd_args):
        '''List project objects.
        Usage:
            list (maps | datasources | services)
        '''

        if cmd_args['maps']:
            print('_______________________\n')
            print('Data maps:\n')
            print('\n'.join([spec.name for spec in self.map_specs]))
            print('_______________________\n')

            
        elif cmd_args['datasources']:
            print('_______________________\n')            
            print('Datasources:\n')
            print('\n'.join(['%s: %s' % (spec.name, spec.klass) for spec in self.datasource_specs]))
            print('_______________________\n')

        elif cmd_args['services']:
            print('placeholder.')
    
    @docopt_cmd
    def do_new(self, cmd_args):
        '''Create a new datamap or map-related object.
        Usage:
             new (map | datasource)
        '''

        if not self.project_home:
            print('\nTo create a new map or datasource, you must specify a valid project home directory.')
            should_update_globals = cli.InputPrompt('Run globals command now (Y/n)?', 'y').show()
            if should_update_globals == 'y':
                self.do_globals({'update': True})
                if not self.project_home:
                    print('project home not updated.')
                    return
                print('Returning to "new map" command...')
            else:
                return

        if not self.datasource_module:
            print('\nTo create a new map or datasource, you must specify the Python module containing your lookup-datasource class.')
            should_update_globals = cli.InputPrompt('Configure now (Y/n)?', 'y').show()
            if should_update_globals == 'y':
                self.do_globals({'update': True})
                if not self.datasource_module:
                    print('datasource module not set.')
                    return
                print('Returning to "new map" command...')
            else:
                return

        if cmd_args['datasource']:
            source_spec = self.configure_datasource(cmd_args)
            if source_spec:
                self.datasource_specs.append(source_spec)
                
        elif cmd_args['map']:
            if self.initial_datafile:
                print('\nThis mkmap CLI session was initialized with datafile %s.\n' % self.initial_datafile)
                
                should_generate_from_datafile = cli.InputPrompt('Use this datafile to generate a map (Y/n)?', 'y').show()
                if should_generate_from_datafile == 'y':
                    separator_char = cli.InputPrompt('separator character used in this file').show()
                    if not len(separator_char): # in case whitespace input (which is valid) is stripped
                        print('Cannot create a CSV map without specifying a separator character.')
                        return

                    print('Will use separator character: ' + separator_char)
                    should_create_source = False
                    if not self.get_current_project_setting('datasource_module'):
                        should_create_source = True
                        print('\nPlease set the lookup source to a valid Python module containing at least one Datasource class.\n')
                        datasource_module = cli.MenuPrompt('Datasource module', generate_module_options_from_directory(os.getcwd()))
                        if not datasource_module:
                            print('Cannot continue without setting the project-wide datasource module.\n')
                            return
                        self.update_project_setting('datasource_module', datasource_module)                        

                    if not len(self.datasource_specs):
                        create_response = cli.InputPrompt('No datasources registered. Register one now (Y/n)?', 'y').show()
                        if create_response == 'y':
                            should_create_source = True
                        else:
                            print('Cannot create a map without a datasource.')
                            return

                    if should_create_source:
                        datasource_spec = self.configure_datasource(cmd_args)
                        if not datasource_spec:
                            print('Cannot create a map without a datasource.')
                            return

                        print('\nRegistered new datasource %s: %s.\n' % (datasource_spec.name, datasource_spec.klass))                        
                        self.datasource_specs.append(datasource_spec)

                    datasource_options = self.generate_datasource_options()
                    selected_source = cli.MenuPrompt('Select a datasource', datasource_options).show()
                    if not selected_source:
                        print('Cannot create a map without a datasource.')
                        return
                    
                    print('\nSelected datasource "%s" for this datamap.\n' % (selected_source))

                    map_name = cli.InputPrompt('Please enter a name for this datamap').show()
                    if not map_name:
                        return
                    
                    print('Scanning initial file with separator character: %s' % separator_char)
                    mapspec = create_default_map_from_csv_file(self.initial_datafile,
                                                               map_name,
                                                               selected_source,
                                                               separator_char)
                    
                    if not mapspec:
                        return
                    confirm = cli.InputPrompt('Create datamap "%s" (Y/n)?' % map_name, 'y').show()
                    if confirm == 'y':
                        print('\nDatamap "%s" created.\n' % map_name)
                        self.map_specs.append(mapspec)
                        # set this to None so that we don't visit this path again
                        self.initial_datafile = None
                else:
                    self.initial_datafile = None                    
            else:
                map_name = cli.InputPrompt('alias for this new map').show()
                if not map_name:
                    return
                mapspec = self.configure_map(map_name)
                if not mapspec:
                    return
                self.map_specs.append(mapspec)
        '''
        elif setting == 'service_module':
            service_spec = self.configure_services(cmd_args)
            if service_spec:
                self.service_objects.append(service_spec)
        '''

    @docopt_cmd
    def do_update(self, cmd_args):
        '''Usage:
                  update (map | project)
                  update map <map_name>
                  update project (globals | datasources)
        '''

        print(common.jsonpretty(cmd_args))


    def get_current_project_setting(self, name):
        for setting in self.globals:
            if setting.name == name:
                return setting.value
        return 
        
    def get_new_project_setting(self, setting_name, current_value=None):
        value = None
        if setting_name == 'project_home':
            value = cli.InputPrompt(setting_name, current_value).show()
        if setting_name == 'datasource_module':
            value = cli.MenuPrompt('Datasource module', generate_module_options_from_directory(os.getcwd())).show()
        if setting_name == 'service_module':
            value = cli.MenuPrompt('Service module', generate_module_options_from_directory(os.getcwd())).show()
        return value


    def update_project_setting(self, name, value):
        for index in range(len(self.globals)):
            if self.globals[index].name == name:
                if name == 'project_home':
                    try:
                        if not value:
                            print('### WARNING: setting project parameter %s to an empty value.' % name)
                        common.load_config_var(value)
                        self.globals[index] = ParamSpec(name=name, value=value)
                    except common.MissingEnvironmentVarException as err:
                        print('\nThe environment variable %s has not been set.' % value)
                        return                    
                else:
                    self.globals[index] = ParamSpec(name=name, value=value)
                break               


    @docopt_cmd
    def do_globals(self, cmd_args):
        '''Usage:
                  globals 
                  globals update [<setting_name>]
        '''

        if cmd_args['update']:
            if not cmd_args['<setting_name>']:
                setting_name = cli.MenuPrompt('update project setting', GLOBAL_OPTIONS).show()        
            else:
                setting_name = cmd_args['<setting_name>']
                
            new_setting_value = self.get_new_project_setting(setting_name,
                                                             self.get_current_project_setting(setting_name))
            if new_setting_value:
                print('\nSetting project parameter "%s" to "%s".\n' % (setting_name, new_setting_value))
                self.update_project_setting(setting_name, new_setting_value)
        
        print('_______________________\n')
        print('Global project settings:\n')
        print('\n'.join(['%s: %s' % (g.name, g.value) for g in self.globals]))
        print('_______________________\n')
        

    def do_show(self, cmd_args):

        if not len(self.datasource_specs):
            print('\nCannot generate a datamap config without one or more lookup datasources.\n')
            return

        if not len(self.map_specs):
            print('\nCannot generate a datamap config without one or more maps.\n')
            return

        
        global_tbl = named_tuple_array_to_dict(self.globals,
                                               key_name='name',
                                               value_name='value')
        template_data = {
            'project_home': global_tbl['project_home'],
            'datasource_module': global_tbl['datasource_module'],
            'service_module': global_tbl['service_module'],
            'datasources': self.datasource_specs,
            'map_specs': self.map_specs
        }

        j2env = jinja2.Environment()
        template_mgr = common.JinjaTemplateManager(j2env)
        map_template = j2env.from_string(MAP_CONFIG_TEMPLATE)
        output_data = map_template.render(**template_data)
        print('_______________________\n')
        print('YAML datamap config:\n')
        print(output_data)
        print('_______________________\n')


    def do_save(self, cmd_args):
        '''Save all datamaps to a file.
        '''

        if not len(self.map_specs):
            print('\nNo datamaps registered. Create one or more datamaps first.\n')
            return
        
        output_filename = cli.InputPrompt('output filename').show()
        global_tbl = named_tuple_array_to_dict(self.globals,
                                               key_name='name',
                                               value_name='value')
        template_data = {
            'project_home': global_tbl['project_home'],
            'datasource_module': global_tbl['datasource_module'],
            'service_module': global_tbl['service_module'],
            'datasources': self.datasource_specs,
            'map_specs': self.map_specs
        }

        j2env = jinja2.Environment()
        template_mgr = common.JinjaTemplateManager(j2env)
        map_template = j2env.from_string(MAP_CONFIG_TEMPLATE)

        output_data = map_template.render(**template_data)
        #print(yaml.dump(data))
        should_save = cli.InputPrompt('Save this datamap configuration to %s (Y/n)?' % output_filename, 'y').show()
        if should_save == 'y':
            with open(output_filename, 'w') as f:
                #f.write(map_template.render(**template_data))
                #yaml.dump(output_data, output_data, f, default_flow_style=False)
                f.write(output_data)
                print('\nSaved datamap config to output file %s.\n' % output_filename)


def main(args):

    j2env = jinja2.Environment()
    template_mgr = common.JinjaTemplateManager(j2env)
    map_template = j2env.from_string(MAP_CONFIG_TEMPLATE)

    source_filename = None
    output_filename = None

    if args['--from'] == True:
        source_filename = args['<datafile>']

    if args['--to'] == True:
        output_filename = args['<output_file>']
        
    prompt = MakeMapCLI('mkmap', initial_sourcefile=source_filename, output_file=output_filename)
    prompt.cmdloop('starting mkmap in interactive mode...')

    '''
    datasource_specs = []
    #datasource_specs.append(DatasourceSpec(name='amc', klass='SVODLookupSource'))
    
    print('enter separator character')
    separator = input()
    source_datafile = args['<datafile>']
    mapspec = create_default_map_from_csv_file(source_datafile, 'foobar', 'amc', '\t')    
    map_specs.append(mapspec)

    template_data = {
        'project_home': '$APOLLO_HOME',
        'datasource_module': 'apollo_datasources',
        'service_module': 'apollo_services',
        'datasources': datasource_specs,
        'map_specs': map_specs
    }

    print(map_template.render(**template_data))
    '''


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

    
