#!/usr/bin/env python

import yaml
from mercury import configtemplates as templates
from mercury.uisequences import *
from mercury.uisequences import UISequenceRunner
from mercury.utils import tab


def load_xfile_config(yaml_config):  
  live_config = {}
  live_config['globals'] = []
  for key, value in yaml_config['globals'].items():
    param = meta.Parameter(name=key, value=value)
    live_config['globals'].append(param)

  live_config['service_objects'] = []
  yaml_svcs = yaml_config.get('service_objects') or []
  for so_name in yaml_svcs:  
    service = meta.ServiceObjectSpec(so_name, yaml_config['service_objects'][so_name]['class'])
    for param in yaml_config['service_objects'][so_name]['init_params']:
      service.add_init_param(param['name'], param['value'])
  
    live_config['service_objects'].append(service)

  live_config['sources'] = []
  sources = yaml_config.get('sources') or []
  for src_name in sources: 
    datasource = meta.DatasourceSpec(src_name, yaml_config['sources'][src_name]['class'])
    live_config['sources'].append(datasource)

  live_config['maps'] = []
  maps = yaml_config.get('maps') or []
  for map_name in maps: 
    xmap = meta.XfileMapSpec(map_name, yaml_config['maps'][map_name]['lookup_source'])

    for setting in yaml_config['maps'][map_name]['settings']:
      # TODO: fill out when we have more than just the default setting
      pass

    for field in yaml_config['maps'][map_name].get('fields') or []:
      field_name = list(field.keys())[0]
      field_params = field[field_name] or {}
      xmap.add_field(field_name, **field_params)

    live_config['maps'].append(xmap)
  
  return live_config


def load_ngst_config(yaml_config):
  live_config = {}
  live_config['globals'] = []
  for key, value in yaml_config['globals'].items():
    param = meta.Parameter(name=key, value=value)
    live_config['globals'].append(param)

  live_config['service_objects'] = []
  yaml_svcs = yaml_config.get('service_objects') or []
  for so_name in yaml_svcs:
    service = meta.ServiceObjectSpec(so_name, yaml_config['service_objects'][so_name]['class'])

    for param in yaml_config['service_objects'][so_name]['init_params']:
      service.add_init_param(param['name'], param['value'])
  
    live_config['service_objects'].append(service)

  live_config['datastores'] = []
  for ds_name in yaml_config['datastores']:
    datastore = meta.NgstDatastore(ds_name, yaml_config['datastores'][ds_name]['class'])

    for param in yaml_config['datastores'][ds_name].get('init_params') or []:      
      datastore.add_init_param(param['name'], param['value'])

    live_config['datastores'].append(datastore)
  
  live_config['targets'] = []
  for target_name in yaml_config['ingest_targets']:
    ds_name = yaml_config['ingest_targets'][target_name]['datastore']
    interval = yaml_config['ingest_targets'][target_name]['checkpoint_interval']
    target = meta.NgstTarget(target_name, ds_name, int(interval))

    live_config['targets'].append(target)
  
  return live_config


def read_globals_array(yaml_config):
  data = []
  for key, value in yaml_config['globals'].items():
    param = meta.Parameter(name=key, value=value)
    data.append(param)
  return data


def read_services_array(yaml_config):
    data = []
    yaml_svcs = yaml_config.get('service_objects') or []
    for so_name in yaml_svcs:
        service = meta.ServiceObjectSpec(so_name, yaml_config['service_objects'][so_name]['class'])
        for param in yaml_config['service_objects'][so_name]['init_params']:
            service.add_init_param(param['name'], param['value'])
        
        data.append(service)
    return data


def load_dfproc_config(yaml_config):
    live_config = {}
    live_config['globals'] = read_globals_array(yaml_config)
    live_config['service_objects'] = read_services_array(yaml_config)
    live_config['processors'] = []

    for processor_name in yaml_config.get('processors', []):
        proc_config = yaml_config['processors'][processor_name]
        read_func = proc_config['read_function']
        transform_func = proc_config['transform_function']
        write_func = proc_config['write_function']
        live_config['processors'].append(meta.DFProcessorSpec(processor_name,
                                                            read_func,
                                                            transform_func,
                                                            write_func))
    return live_config                                                        
  

def load_j2sqlgen_config(yaml_config):
    live_config = {}
    live_config['globals'] = read_globals_array(yaml_config)
    live_config['service_objects'] = read_services_array(yaml_config)

    defaults_spec = meta.J2SqlGenDefaultsSpec(**yaml_config['defaults'])
    
    #defaults_spec.set_table_suffix(yaml_config['defaults'].get('table_suffix'))
    #defaults_spec.set_column_suffix(yaml_config['defaults'].get('column_suffix'))

    live_config['defaults'] = [defaults_spec]
    live_config['tables'] = []
    if yaml_config['tables'] is not None:
      for tablename in yaml_config['tables']:

          table_map_spec = meta.J2SqlGenTableMapSpec(tablename)

          table_config = yaml_config['tables'][tablename]
          rename_target = table_config.get('rename_to')
          if rename_target:
              table_map_spec.set_rename_target(rename_target)

          if table_config.get('column_name_map') is not None:
            for old_name, new_name in table_config['column_name_map'].items():
                table_map_spec.remap_column(old_name, new_name)

         
          if table_config.get('column_settings') is not None:
            for colname in table_config['column_settings']:                
                column_settings = table_config['column_settings'][colname]                
                table_map_spec.add_column_settings(colname, **column_settings)

          live_config['tables'].append(table_map_spec)
          
    return live_config


def load_quasr_config(yaml_config):
    live_config = {}
    live_config['globals'] = read_globals_array(yaml_config)
    live_config['service_objects'] = read_services_array(yaml_config)

    live_config['templates'] = []
    for template_name in yaml_config.get('templates', []):
        live_config['templates'].append(meta.QuasrTemplateSpec(template_name,
                                                            yaml_config['templates'][template_name]))                                                           
  
    live_config['jobs'] = []
    for job_name in yaml_config['jobs']:
        job = meta.QuasrJobSpec(job_name, yaml_config['jobs'][job_name]['sql_template'])
        yaml_input_slots = yaml_config['jobs'][job_name].get('inputs') or []
        for slot in yaml_input_slots:
            job.add_input_slot(slot['name'], slot['type'])

        yaml_output_slots = yaml_config['jobs'][job_name].get('outputs') or []
        for slot in yaml_output_slots:
            job.add_output_slot(slot['name'], slot['type'])
        
        job.executor_function = yaml_config['jobs'][job_name]['executor_function']
        job.builder_function = yaml_config['jobs'][job_name]['builder_function']
        job.analyzer_function = yaml_config['jobs'][job_name].get('analyzer_function') or  ''
        live_config['jobs'].append(job)

    return live_config


def load_cyclops_config(yaml_config):
    live_config = {}
    live_config['globals'] = read_globals_array(yaml_config)
    live_config['service_objects'] = read_services_array(yaml_config)
    live_config['triggers'] = []

    for trigger_name in yaml_config.get('triggers', []):
        trigger_config = yaml_config['triggers'][trigger_name]

        event_type = trigger_config['event_type']
        directory = trigger_config['parent_dir']
        handler_func = trigger_config['function']
        filename_filter = trigger_config.get('filename_filter')

        trigger = meta.CyclopsTrigger(trigger_name,
                                            event_type,
                                            directory,
                                            handler_func,
                                            filename_filter)
        live_config['triggers'].append(trigger)

    return live_config


def load_pgexec_config(yaml_config):
    live_config = {}
    live_config['targets'] = []

    for target_name in yaml_config['targets']:        
        target_config = yaml_config['targets'][target_name]
        name = target_config['name']
        host = yaml_config['host']
        port = int(yaml_config.get('port', 5432))
        username = yaml_config['user']
        password = yaml_config['password']

        settings = {}
        for item in target_config.get('settings', []):
            settings[item['name']] = item['value']
    
        target = meta.PGExecTargetSpec(target_name,
                                       host,
                                       port,
                                       username,
                                       password,
                                       **settings)
        live_config[targets].append(target)

    return live_config

def load_profilr_config(yaml_config):
    live_config = {}
    live_config['globals'] = read_globals_array(yaml_config)
    live_config['service_objects'] = read_services_array(yaml_config)

    live_config['profilers'] = []
    for pname in yaml_config['profilers']:
        profiler_class = yaml_config['profilers'][pname]['class']
        pspec = meta.ProfilerSpec(pname, profiler_class)
        live_config['profilers'].append(pspec)

    live_config['datasets'] = []
    for dataset_name in yaml_config['datasets']:
        dataset_config = yaml_config['datasets'][dataset_name]
        profiler_name = dataset_config['profiler']
        settings = dataset_config.get('settings', {})
    
        dataset_spec = meta.ProfilerDatasetSpec(dataset_name, profiler_name, **settings)

        for field in dataset_config['fields']:
            dataset_spec.add_field(field)
        
        for fieldname in dataset_config.get('null_equivalents', []):
            values = dataset_config['null_equivalents'][fieldname]
            dataset_spec.add_null_equivalents(field_name, *values)

        live_config['datasets'].append(dataset_spec)

    return live_config


def find_global():
  pass

def find_service_object():
  pass

def find_dfproc_processor():
  pass

def find_xfile_map():
  pass

def find_xfile_datasource():
  pass

def find_ngst_datastore(name):
  pass

def find_ngst_target_by_name():
  pass

def find_cyclops_trigger_by_name():
  pass

def find_j2sqlgen_table_by_name():
  pass

def find_j2sqlgen_default_by_name():
  pass

def find_pgexec_target_by_name():
  pass

def find_profiler():
  pass

def find_profilr_dataset_by_name():
  pass

def find_quasr_template():
  pass


def find_quasr_job(name, live_config, config_target):
  for job in live_config['jobs']:
    if getattr(job, config_target['index_attribute']) == name:
      return job
  
  return None


def create_xfile_globals(live_config, target_package):
  # this will return a dictionary containing all global parameters for an xfile project
  result =  UISequenceRunner(configuration=live_config).create(**xfile_globals_create_sequence)

  # return all Parameter specs
  for key, value in result.items():
    yield meta.Parameter(name=key, value=value)


def create_service_object(live_config, target_package):
  data = UISequenceRunner(configuration=live_config).create(**service_object_create_sequence)
  if data:
    so_spec = meta.ServiceObjectSpec(data['alias'], data['class'])
    add_params = cli.InputPrompt('Add one or more init params (Y/n)?', 'y').show()
    if add_params.lower() == 'y':
      while True:
        param_data = UISequenceRunner(configuration=live_config).create(**service_object_param_sequence)
        if param_data:
          so_spec.add_init_param(param_data['name'], param_data.get('value', ''))
          answer = cli.InputPrompt('add another parameter (Y/n)?', 'y').show()
          should_continue = answer.lower()
          if should_continue != 'y':
              break
        else:
          break
    yield so_spec

  return None

  
def create_dfproc_globals(live_config, target_package):
  pass

def create_dfproc_processor(live_config, target_package):
  pass


def create_xfile_datasource(live_config, target_package):
  yield UISequenceRunner(configuration=live_config).create(**xfile_datasource_create_sequence)
  
  
def create_xfile_map(live_config, target_package):

  if not live_config.get('sources'):
    print('!!! No datasources registered. Please create a datasource first.')
    return None

  # look up existing datasources in the live configuration
  datasource_select_menudata = []
  for datasource in live_config['sources']:
    datasource_select_menudata.append({'label': datasource.name, 'value': datasource.name})

  # use this generated menu prompt in the uisequence step whose field_name is "lookup_source"
  prompts = {
    'lookup_source': cli.MenuPrompt('lookup datasource', datasource_select_menudata)
  }

  mapspec = UISequenceRunner(create_prompts=prompts,
                             configuration=live_config).create(**xfile_map_create_sequence)
  while True:
    fieldspec = UISequenceRunner(configuration=live_config).create(**xfile_field_create_sequence)
    if not fieldspec:
      break
    mapspec.add_field_spec(fieldspec)
    answer = cli.InputPrompt('add another field (Y/n)?').show()
    should_continue = answer.lower()
    if should_continue == 'n':
      break
  
  yield mapspec


def create_ngst_globals(live_config, target_package):
  # this will return a dictionary containing all global parameters for an xfile project
  result =  UISequenceRunner(configuration=live_config).create(**ngst_globals_create_sequence)
  if not result:
    return
    
  # return all Parameter specs
  for key, value in result.items():
    yield meta.Parameter(name=key, value=value)
  

def create_ngst_target(live_config, target_package):

  prompts = {}
  menudata = []
  if not len(live_config.get('datastores', [])):
    print('No datastores registered. You must register at least one datastore to create a target.')
    return

  for dstore in live_config['datastores']:
    menudata.append({'label': dstore.alias, 'value': dstore.alias})
  
  prompts['datastore_alias'] = cli.MenuPrompt('select a datastore', menudata)

  while True:
    data = UISequenceRunner(create_prompts=prompts,
                            configuration=live_config).create(**ngst_target_create_sequence)    
    if not data:
      break

    target = meta.NgstTarget(data['name'], data['datastore_alias'], int(data['checkpoint_interval']))
    yield target
    answer = cli.InputPrompt('create another target (Y/n)?').show()
    should_continue = answer.lower()
    if should_continue == 'n':
      break


def create_ngst_datastore(live_config, target_package):
  while True:
    data = UISequenceRunner(configuration=live_config).create(**ngst_datastore_create_sequence)
    if not data:
      break
    dstore = meta.NgstDatastore(data['alias'], data['classname'])
    dstore.channel_selector_function = data['channel_selector_function']
    yield dstore

    answer = cli.InputPrompt('create another datastore (Y/n)?').show()
    should_continue = answer.lower()
    if should_continue == 'n':
      break


def create_cyclops_trigger(live_config, target_package):
  pass

def create_cyclops_globals(live_config, target_package):
  pass

def create_j2sqlgen_globals(live_config, target_package):
  # this will return a dictionary containing all global parameters for a j2sqlgen project
  result =  UISequenceRunner(configuration=live_config).create(**j2sqlgen_globals_create_sequence)
  # return all Parameter specs
  for key, value in result.items():
    yield meta.Parameter(name=key, value=value)  
  

def create_j2sqlgen_defaults(live_config, target_package):
  settings = UISequenceRunner(configuration=live_config).create(**j2sqlgen_defaults_create_sequence)
  column_map = {}
  while True:
    mapping = UISequenceRunner(configuration=live_config).create(**j2sqlgen_colmap_create_sequence)
    if not mapping:
      break
    
    column_map[mapping['source_type']] = mapping['target_type']

    answer = cli.InputPrompt('create another column mapping (Y/n)?').show()
    should_continue = answer.lower()
    if should_continue == 'n':
      break

  settings['column_type_map'] = column_map
  yield meta.J2SqlGenDefaultsSpec(**settings)
  

def create_j2sqlgen_table(live_config, target_package):
  tablespec = UISequenceRunner(configuration=live_config).create(**j2sqlgen_table_create_sequence)
  yield tablespec


def create_pgexec_target(live_config, target_package):
  pass

def create_profilr_globals(live_config, target_package):
  pass

def create_profilr_profiler(live_config, target_package):
  pass

def create_profilr_dataset(live_config, target_package):
  pass

def create_quasr_template(live_config, target_package):
  while True:
    template_spec = UISequenceRunner(configuration=live_config).create(**quasr_template_create_sequence)
    if not template_spec:
      break

    yield template_spec
    answer = cli.InputPrompt('create another template (Y/n)?').show()
    should_continue = answer.lower()
    if should_continue == 'n':
      break


def create_quasr_job(live_config, target_package):
  while True:
    jobspec = UISequenceRunner(configuration=live_config).create(**quasr_job_create_sequence)
    yield jobspec
    answer = cli.InputPrompt('create another job (Y/n)?').show()
    should_continue = answer.lower()
    if should_continue == 'n':
      break


def create_quasr_globals(live_config, target_package):
  # this will return a dictionary containing all global parameters for a quasr project
  result =  UISequenceRunner(configuration=live_config).create(**quasr_globals_create_sequence)  
  for key, value in result.items():
    yield meta.Parameter(name=key, value=value)


def edit_globals(live_config, setting):
  if not setting:
    return
  UISequenceRunner(configuration=live_config).process_edit_sequence(setting, **parameter_edit_sequence)
             

def edit_service_object(live_config, service_object):
  if not service_object:
    return
  UISequenceRunner(configuration=live_config).process_edit_sequence(service_object, **service_object_edit_sequence)


def edit_dfproc_processor(live_config, processor):
  pass


def edit_xfile_datasource(live_config, config_object):
  if not config_object:
    return
  UISequenceRunner(configuration=live_config).process_edit_sequence(config_object, **xfile_datasource_edit_sequence)


def edit_xfile_map(live_config, mapspec):
  if not mapspec:
    return

  mapfield_menudata = []
  for fieldspec in mapspec.fields:
    mapfield_menudata.append({'label': fieldspec.name, 'value': fieldspec.name})

  menus = {
    'fields': mapfield_menudata
  }
  UISequenceRunner(configuration=live_config, edit_menus=menus).edit(mapspec, **xfile_map_edit_sequence)


def edit_quasr_job(live_config, job):
  if not job:
    return
  input_slot_menu = [{'label': slot.name, 'value': slot.name} for slot in job.inputs]
  output_slot_menu = [{'label': slot.name, 'value': slot.name} for slot in job.outputs]
  menus = {
    'inputs': input_slot_menu,
    'outputs': output_slot_menu
  }
  UISequenceRunner(configuration=live_config, edit_menus=menus).edit(job, **quasr_job_edit_sequence)


def edit_ngst_datastore(live_config, datastore):
  if not datastore:
    return
  # load menu data with this target's init params, for editing
  iparam_menudata = []
  for param in datastore.init_params:
    iparam_menudata.append({'label': param.name, 'value': param.name}) 

  menus = {
    'init_params': iparam_menudata
  }
  
  UISequenceRunner(configuration=live_config, edit_menus=menus).edit(datastore, **ngst_datastore_edit_sequence)


def edit_ngst_target(live_config, edit_target):
  if not edit_target:
    return

  datastore_menudata = []
  for dstore in live_config['datastores']:
    datastore_menudata.append({'label': dstore.alias, 'value': dstore.alias})
  menus = {
    'datastore_alias': datastore_menudata
  }
  UISequenceRunner(configuration=live_config, edit_menus=menus).edit(edit_target, **ngst_target_edit_sequence)


def edit_cyclops_trigger():
  pass

def edit_j2sqlgen_defaults():
  pass

def edit_j2sqlgen_table():
  pass

def edit_pgexec_target():
  pass

def edit_profiler():
  pass

def edit_profilr_dataset():
  pass

def edit_quasr_template(live_config, template_spec):
  if not template_spec:
    return
  UISequenceRunner(configuration=live_config).process_edit_sequence(template_spec, **quasr_template_edit_sequence)


def list_globals(global_settings):
  if not len(global_settings):
    print('No globals registered.')
    return
  
  print('globals:')
  for gs in global_settings:
    print('%s%s: %s' % (tab(1), gs.name, gs.value))
  

def list_service_objects(service_objects):
  if not len(service_objects):
    print('No service objects registered.')
    return

  print('service_objects:')
  for so in service_objects:
    print('%s%s:' % (tab(1), so.alias))
    print('%sclass: %s' % (tab(2), so.classname))
    print('%sinit_params:' % tab(2))
    for p in so.init_params:
      print('%s- name: %s' % (tab(3), p.name))
      print('%s  value: %s' % (tab(3), p.value))


def list_dfproc_processors():
  pass


def list_xfile_datasources(datasources):
  if not len(datasources):
    print("No datasources registered.")
    return
  print('datasources:')
  for src in datasources:
    print('%s%s:' % (tab(1), src.name))
    print('%sclass: %s' % (tab(2), src.classname))    
  

def list_xfile_maps(maps):
  if not len(maps):
    print("No maps registered.")
    return

  print('maps:')
  for mapspec in maps:
    print('%s%s:' % (tab(1), mapspec.name))
    for field in mapspec.fields:
      print('%s- %s:' % (tab(2), field.name))
      for param in field.parameters:
        print('%s%s: %s' % (tab(3), param.name, param.value))


def list_ngst_datastores(datastores):
  if not len(datastores):
    print('No datastores registered.')
    return

  print('datastores:')
  for ds in datastores:
    print('%s%s:' % (tab(1), ds.alias))
    print('%sclass: %s' % (tab(2), ds.classname))
    print(tab(2) + 'init_params:')
    for param in ds.init_params:
      print('%s- name: %s' % (tab(3), param.name))
      print('%s  value: %s' % (tab(3), param.value))
  

def list_ngst_targets(targets):
  if not len(targets):
    print('No targets registered.')
    return

  print('ingest_targets:')
  for t in targets:
    print('%s%s:' % (tab(1), t.name))
    print('%sdatastore: %s' % (tab(2), t.datastore_alias))
    print('%scheckpoint_interval: %s' % (tab(2), t.checkpoint_interval))


def list_cyclops_triggers():
  pass


def list_j2sqlgen_defaults(defaults_spec):
  if not len(defaults_spec.settings):
    print('No settings registered.')
    return
  
  print('defaults:')
  for key in defaults_spec.settings:
    param = defaults_spec.settings[key]
    print('%s%s: %s' % (tab(1), key, param.value))

  print('\n')  
  print(tab(1) + 'column_type_map:')
  for k,v in defaults_spec.column_type_map.items(): # this is a dictionary
    print('%s%s: %s' % (tab(2), k, v))


def list_j2sqlgen_tables(tablemap_specs):
  print('tables:')
  for spec in tablemap_specs:
    print('%s%s:' % (tab(1), spec.table_name))
    if spec.rename_to:
      print(tab(2) + 'rename_to: ' + spec.rename_to)

    if len(spec.column_settings.keys()):
      print(tab(2) + 'column_settings:')
      for key, settings in spec.column_settings.items():
        print(tab(3) + key + ':')
        # the value of <settings> is a dictionary
        for name, value in settings.items():
          print('%s%s: %s' % (tab(4), name, value))

    if len(spec.column_rename_map.keys()):
      print(tab(2) + 'column_name_map:')
      for key, value in spec.column_rename_map.items():
        print('%s%s: %s' % (tab(3), key, str(value)))

    print('\n')


def list_pgexec_targets():
  pass

def list_profilers():
  pass

def list_profilr_datasets():
  pass


def list_quasr_templates(templates):
  if not len(templates):
    print('No templates registered.')
    return

  print('templates:')
  for t in templates:
    print('%s%s:' % (tab(1), t.name))
    for line in t.text.split('\n'):
      print(tab(2) + line)
    #print('\n')


def list_quasr_jobs(jobs):
  if not len(jobs):
    print("No jobs registered.")
    return

  print('jobs:')
  for job in jobs:
    print('%s%s:' % (tab(1), job.name))
    print(tab(2) + 'input slots:')
    for slot in job.inputs:
      print('%s%s (%s)' % (tab(3), slot.name, slot.datatype))
    print('\n')
    print(tab(2) + 'output slots:')
    for slot in job.outputs:
      print('%s%s (%s)' % (tab(3), slot.name, slot.datatype))
    print('\n')


def validate_xfile_config(yaml_string, live_config):
  errors = []
  yaml_config = yaml.safe_load(yaml_string)
  print(common.jsonpretty(yaml_config))
  if not 'globals' in yaml_config.keys():
    errors.append('config is missing a "globals" section')

  if not yaml_config['globals']:
    errors.append('"globals" section is empty')
  if not 'service_objects' in yaml_config.keys():
    errors. append('config is missing a "service_objects" section')
  if not 'sources' in yaml_config.keys():
    errors.append('config is missing a "sources" section')
  if not 'maps' in yaml_config.keys():
    errors.append('config is missing a "maps" section')

  if yaml_config['maps']:
    for map_name in yaml_config['maps']:
      if not yaml_config['maps'][map_name].get('lookup_source'):
        errors.append('the map "%s" does not specify a lookup_source')
      source_name = yaml_config['maps'][map_name]['lookup_source']
      if source_name not in yaml_config['sources'].keys():
        errors.append('map "%s" specifies an unregistered lookup_source "%s"' 
                      % (map_name, source_name))

  if len(errors):
    return (False, errors)
  else:
    return (True, [])


targets = {
  'dfproc': {
    'description': 'Create and transform Pandas dataframes',
    'template': templates.DFPROC_TEMPLATE,
    'loader': load_dfproc_config, 
    'config_object_types': [
      {
        'name': 'globals',
        'singular_label': 'globals',
        'plural_label': 'globals',
        'index_attribute': 'name',
        'find_func': find_global,
        'create_func': create_dfproc_globals,
        'update_func': edit_globals,
        'list_func': list_globals
      },
      {
        'name': 'service_objects',
        'singular_label': 'service',
        'plural_label': 'services',
        'index_attribute': 'alias',
        'find_func': find_service_object,
        'create_func': create_service_object,
        'update_func': edit_service_object,
        'list_func': list_service_objects
      },
      {
        'name': 'processors',
        'singular_label': 'processor',
        'plural_label': 'processors',
        'index_attribute': 'name',
        'find_func': find_dfproc_processor,
        'create_func': create_dfproc_processor,
        'update_func': edit_dfproc_processor,
        'list_func': list_dfproc_processors
      }
    ]
  },
  'xfile': {
    'description': 'Read and transform CSV or JSON records',
    'template': templates.XFILE_TEMPLATE,
    'validator_func': validate_xfile_config,
    'loader': load_xfile_config,
    'config_object_types': [
      {
        'name': 'globals',
        'singular_label': 'globals',
        'plural_label': 'globals',
        'index_attribute': 'name',
        'find_func': find_global,
        'create_func': create_xfile_globals,
        'update_func': edit_globals,
        'list_func': list_globals,
        'unit_size': 3,
        'singleton': True
      },
      {
        'name': 'service_objects',
        'singular_label': 'service',
        'plural_label': 'services',
        'index_attribute': 'alias',
        'find_func': find_service_object,
        'create_func': create_service_object,
        'update_func': edit_service_object,
        'list_func': list_service_objects        
      },
      {
        'name': 'sources', 
        'singular_label': 'datasource',
        'plural_label': 'datasources',
        'index_attribute': 'name',
        'find_func': find_xfile_datasource,
        'create_func': create_xfile_datasource,
        'update_func': edit_xfile_datasource,
        'list_func': list_xfile_datasources
      },
      {
        'name': 'maps',
        'singular_label': 'map',
        'plural_label': 'maps',
        'index_attribute': 'name',
        'find_func': find_xfile_map,
        'create_func': create_xfile_map,
        'update_func': edit_xfile_map,
        'list_func': list_xfile_maps
      } 
    ]
  },
  'ngst': {
    'description': 'Send CSV or JSON records to a designated target',
    'template': templates.NGST_TEMPLATE,
    'loader': load_ngst_config,
    'config_object_types': [
      {
        'name': 'globals',
        'singular_label': 'globals',
        'plural_label': 'globals',
        'index_attribute': 'name',
        'find_func': find_global,
        'create_func': create_ngst_globals,
        'update_func': edit_globals,
        'list_func': list_globals
      },
      {
        'name': 'service_objects',
        'singular_label': 'service',
        'plural_label': 'services',
        'index_attribute': 'alias',
        'find_func': find_service_object,
        'create_func': create_service_object,
        'update_func': edit_service_object,
        'list_func': list_service_objects        
      },
      {
        'name': 'datastores',
        'singular_label': 'datastore',
        'plural_label': 'datastores',
        'index_attribute': 'alias',
        'find_func': find_ngst_datastore,
        'create_func': create_ngst_datastore,
        'update_func': edit_ngst_datastore,
        'list_func': list_ngst_datastores
      },
      {
        'name': 'targets',
        'singular_label': 'target',
        'plural_label': 'targets',
        'index_attribute': 'name',
        'find_func': find_ngst_target_by_name,
        'create_func': create_ngst_target,
        'update_func': edit_ngst_target,
        'list_func': list_ngst_targets
      }
    ]
  },
  'cyclops': {
    'description': 'Run custom code in response to filesystem events',
    'template': templates.CYCLOPS_TEMPLATE,    
    'loader': load_cyclops_config,
    'config_object_types': [
      {
        'name': 'globals',
        'singular_label': 'globals',
        'plural_label': 'globals',
        'index_attribute': 'name',
        'find_func': find_global,
        'create_func': create_cyclops_globals,
        'update_func': edit_globals,
        'list_func': list_globals
      },
      {
        'name': 'service_objects',
        'singular_label': 'service',
        'plural_label': 'services',
        'index_attribute': 'alias',
        'find_func': find_service_object,
        'create_func': create_service_object,
        'update_func': edit_service_object,
        'list_func': list_service_objects        
      },
      {
        'name': 'triggers',
        'singular_label': 'trigger',
        'plural_label': 'triggers',
        'index_attribute': 'name',
        'find_func': find_cyclops_trigger_by_name,
        'create_func': create_cyclops_trigger,
        'update_func': edit_cyclops_trigger,
        'list_func': list_cyclops_triggers
      }
    ]
  },
  'j2sqlgen': {
    'description': 'Generate CREATE TABLE sql statements from JSON metadata',
    'template': templates.J2SQLGEN_TEMPLATE,
    'loader': load_j2sqlgen_config,
    'config_object_types': [
      {
        'name': 'globals',
        'singular_label': 'globals',
        'plural_label': 'globals',
        'index_attribute': 'name',
        'find_func': find_global,
        'create_func': create_j2sqlgen_globals,
        'update_func': edit_globals,
        'list_func': list_globals
      },
      {
        'name': 'defaults',
        'singular_label': 'defaults',
        'plural_label': 'defaults',
        'find_func': find_j2sqlgen_default_by_name,
        'create_func': create_j2sqlgen_defaults,
        'update_func': edit_j2sqlgen_defaults,
        'list_func': list_j2sqlgen_defaults,
        'unit_size': 1,
        'singleton': True
      },
      {
        'name': 'tables',
        'singular_label': 'table',
        'plural_label': 'tables',
        'find_func': find_j2sqlgen_table_by_name,
        'create_func': create_j2sqlgen_table,
        'update_func': edit_j2sqlgen_table,
        'list_func': list_j2sqlgen_tables
      }
    ]
  },
  'pgexec': {
    'description': 'Execute SQL commands against a PostgreSQL database',
    'template': templates.PGEXEC_TEMPLATE,
    'loader': load_pgexec_config,
    'config_object_types': [
        {
          'name': 'targets',
          'singular_label': 'target',
          'find_func': find_pgexec_target_by_name,
          'create_func': create_pgexec_target,
          'update_func': edit_pgexec_target,
          'list_func': list_pgexec_targets
        },
      ]
  },
  'profilr': {
    'description': 'Run custom data profiling logic against a file-based dataset',
    'template': templates.PROFILR_TEMPLATE,
    'loader': load_profilr_config,
    'config_object_types': [
        {
        'name': 'globals',
        'singular_label': 'global',
        'find_func': find_global,
        'create_func': create_profilr_globals,
        'update_func': edit_globals,
        'list_func': list_globals
      },
      {
        'name': 'service_objects',
        'singular_label': 'service object',
        'index_attribute': 'alias',
        'find_func': find_service_object,
        'create_func': create_service_object,
        'update_func': edit_service_object,
        'list_func': list_service_objects
      },
      {
        'name': 'profilers',
        'singular_label': 'profiler',
        'index_attribute': 'name',
        'find_func': find_profiler,
        'create_func': create_profilr_profiler,
        'update_func': edit_profiler,
        'list_func': list_profilers
      },
      {
        'name': 'datasets',
        'singular_label': 'profilr dataset',
        'find_func': find_profilr_dataset_by_name,
        'create_func': create_profilr_dataset,
        'update_func': edit_profilr_dataset,
        'list_func': list_profilr_datasets
      }
    ]
  },
  'quasr': {
    'description': 'Run custom QA/profiling code against a relational dataset',
    'template': templates.QUASR_TEMPLATE,
    'loader': load_quasr_config,
    'config_object_types': [
      {
        'name': 'globals',
        'singular_label': 'globals',
        'plural_label': 'globals',
        'index_attribute': 'name',
        'find_func': find_global,
        'create_func': create_quasr_globals,
        'update_func': edit_globals,
        'list_func': list_globals,
        'unit_size': 4,
        'singleton': True      
      },
      {
          'name': 'service_objects',
          'singular_label': 'service',
          'plural_label': 'service_objects',
          'index_attribute': 'alias',
          'find_func': find_service_object,
          'create_func': create_service_object,
          'update_func': edit_service_object,
          'list_func': list_service_objects
      },
      {
          'name': 'templates',
          'singular_label': 'template',
          'plural_label': 'templates',
          'index_attribute': 'name',
          'find_func': find_quasr_template,
          'create_func': create_quasr_template,
          'update_func': edit_quasr_template,
          'list_func': list_quasr_templates
      },
      {
          'name': 'jobs',
          'singular_label': 'job',
          'plural_label': 'jobs',
          'index_attribute': 'name',
          'find_func': find_quasr_job,
          'create_func': create_quasr_job,
          'update_func': edit_quasr_job,
          'list_func': list_quasr_jobs
      }
    ]
  }
}