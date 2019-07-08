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

  yaml_svcs = yaml_config.get('service_objects') or []
  for so_name in yaml_svcs:  
    service = meta.ServiceObjectSpec(so_name, yaml_config['service_objects'][so_name]['class'])
    for param in yaml_config['service_objects'][so_name]['init_params']:
      service.add_init_param(param['name'], param['value'])
  
    live_config['service_objects'].append(service)

  live_config['sources'] = []
  for src_name in yaml_config['sources']:
    datasource = meta.DatasourceSpec(src_name, yaml_config['sources'][src_name]['class'])
    live_config['sources'].append(datasource)

  live_config['maps'] = []
  for map_name in yaml_config['maps']:
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
  
  live_config['ingest_targets'] = []
  for target_name in yaml_config['ingest_targets']:
    ds_name = yaml_config['ingest_targets'][target_name]['datastore']
    interval = yaml_config['ingest_targets'][target_name]['checkpoint_interval']
    target = meta.NgstTarget(target_name, ds_name, int(interval))

    live_config['ingest_targets'].append(target)
  
  return live_config


def load_quasr_config(yaml_config):
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

def find_j2sqlgen_target_by_name():
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

def find_quasr_job():
  pass


def create_xfile_globals(live_config, target_package):
  # this will return a dictionary containing all global parameters for an xfile project
  result =  UISequenceRunner().create(**xfile_globals_create_sequence)

  # return all Parameter specs
  for key, value in result.items():
    yield meta.Parameter(name=key, value=value)


def create_service_object(live_config, target_package):
  data = UISequenceRunner().create(**service_object_create_sequence)
  if data:
    so_spec = meta.ServiceObjectSpec(data['alias'], data['class'])
    add_params = cli.InputPrompt('Add one or more init params (Y/n)?', 'y').show()
    if add_params.lower() == 'y':
      while True:
        param_data = UISequenceRunner().create(**service_object_param_sequence)
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
  data = UISequenceRunner().create(**xfile_datasource_create_sequence)
  if data:
    yield meta.DatasourceSpec(data['alias'], data['class'])
  return None
  

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

  mapspec = UISequenceRunner(create_prompts=prompts).create(**xfile_map_create_sequence)
  while True:
    fieldspec = UISequenceRunner().create(**xfile_field_create_sequence)
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
  result =  UISequenceRunner().create(**ngst_globals_create_sequence)

  # return all Parameter specs
  for key, value in result.items():
    yield meta.Parameter(name=key, value=value)
  

def create_ngst_target(live_config, target_package):

  prompts = {}
  menudata = []
  for dstore in live_config['datastores']:
    menudata.append({'label': dstore.alias, 'value': dstore.alias})
  
  prompts['datastore_alias'] = cli.MenuPrompt('select a datastore', menudata)

  while True:
    data = UISequenceRunner(create_prompts=prompts).create(**ngst_target_create_sequence)    
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
    data = UISequenceRunner().create(**ngst_datastore_create_sequence)
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
  pass

def create_j2sqlgen_default(live_config, target_package):
  pass

def create_j2sqlgen_target(live_config, target_package):
  pass

def create_pgexec_target(live_config, target_package):
  pass

def create_profilr_globals(live_config, target_package):
  pass

def create_profilr_profiler(live_config, target_package):
  pass

def create_profilr_dataset(live_config, target_package):
  pass

def create_quasr_template(live_config, target_package):
  pass


def create_quasr_job(live_config, target_package):
  while True:
    jobspec = UISequenceRunner().create(**quasr_job_create_sequence)
    yield jobspec
    answer = cli.InputPrompt('create another job (Y/n)?').show()
    should_continue = answer.lower()
    if should_continue == 'n':
      break


def create_quasr_globals(live_config, target_package):
  # this will return a dictionary containing all global parameters for a quasr project
  result =  UISequenceRunner().create(**quasr_globals_create_sequence)  
  for key, value in result.items():
    yield meta.Parameter(name=key, value=value)


def edit_globals(live_config, setting):
  UISequenceRunner().process_edit_sequence(setting, **parameter_edit_sequence)
             

def edit_service_object(live_config, service_object):
  UISequenceRunner().process_edit_sequence(service_object, **service_object_edit_sequence)


def edit_dfproc_processor(live_config, processor):
  pass


def edit_xfile_datasource(live_config, config_object):  
  UISequenceRunner().process_edit_sequence(config_object, **xfile_datasource_edit_sequence)


def edit_xfile_map(live_config, mapspec):  
  UISequenceRunner().process_edit_sequence(mapspec, **xfile_map_edit_sequence)


def edit_quasr_job(live_config, job):
  #for obj in config_objects:
  input_slot_menu = [{'label': slot.name, 'value': slot.name} for slot in job.inputs]
  output_slot_menu = [{'label': slot.name, 'value': slot.name} for slot in job.outputs]
  menus = {
    'inputs': input_slot_menu,
    'outputs': output_slot_menu
  }
  UISequenceRunner(edit_menus=menus).edit(job, **quasr_job_edit_sequence)


def edit_ngst_datastore(live_config, datastore):
  '''
  menudata = []
  for d in datastores:
    menudata.append({'label': d.alias, 'value': d})

  edit_target = cli.MenuPrompt('Select a datastore to edit', menudata).show()
  if edit_target:
  '''
  # load menu data with this target's init params, for editing
  iparam_menudata = []
  for param in datastore.init_params:
    iparam_menudata.append({'label': param.name, 'value': param.name}) 

  menus = {
    'init_params': iparam_menudata
  }
  
  UISequenceRunner(edit_menus=menus).edit(datastore, **ngst_datastore_edit_sequence)


def edit_ngst_target(live_config, edit_target):
  '''
  menudata = []
  for t in targets:
    menudata.append({'label': t.name, 'value': t})
  
  edit_target = cli.MenuPrompt('Select a target to edit', menudata).show()
  '''
  if edit_target:

    datastore_menudata = []
    for dstore in live_config['datastores']:
      datastore_menudata.append({'label': dstore.alias, 'value': dstore.alias})
    
    menus = {
      
      'datastore_alias': datastore_menudata
    }
    UISequenceRunner(edit_menus=menus).edit(edit_target, **ngst_target_edit_sequence)


def edit_cyclops_trigger():
  pass

def edit_j2sqlgen_default():
  pass

def edit_j2sqlgen_target():
  pass

def edit_pgexec_target():
  pass

def edit_profiler():
  pass

def edit_profilr_dataset():
  pass

def edit_quasr_template():
  pass


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

def list_j2sqlgen_defaults():
  pass

def list_j2sqlgen_targets():
  pass

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
        'name': 'ingest_targets',
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
        'singular_label': 'filesystem event trigger',
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
        'singular_label': 'SQL generation default',
        'find_func': find_j2sqlgen_default_by_name,
        'create_func': create_j2sqlgen_default,
        'update_func': edit_j2sqlgen_default,
        'list_func': list_j2sqlgen_defaults
      },
      {
        'name': 'tables',
        'singular_label': 'j2sql target table',
        'find_func': find_j2sqlgen_target_by_name,
        'create_func': create_j2sqlgen_target,
        'update_func': edit_j2sqlgen_target,
        'list_func': list_j2sqlgen_targets
      }
    ]
  },
  'pgexec': {
    'description': 'Execute SQL commands against a PostgreSQL database',
    'template': templates.PGEXEC_TEMPLATE,
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
  'pgmeta': {
    'description': 'Extract table metadata as JSON from a PostgreSQL database',
    'template': templates.PGMETA_TEMPLATE,
    'config_object_types': [
        {
          'name': 'targets',
          'singular_label': 'target',
          'find_func': find_pgexec_target_by_name, # pgmeta and pgexec use the same target
          'create_func': create_pgexec_target,
          'update_func': edit_pgexec_target,
          'list_func': list_pgexec_targets
        }
      ]
  },
  'profilr': {
    'description': 'Run custom data profiling logic against a file-based dataset',
    'template': templates.PROFILR_TEMPLATE,
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
          'plural_label': 'services',
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