#!/usr/bin/env python

import copy
from snap import cli_tools as cli
from snap import common
from mercury import metaobjects as meta


class MultilineInputPrompt(object):
    def __init__(self, prompt_string):  
      self.prompt = '>> %s: ' % prompt_string
        
    def show(self):
      input_lines = []
      print('%s (hit <enter> on an empty line to finish):\n' % self.prompt)
      while True: 
        result = input().strip()
        if not len(result):
          break
        input_lines.append(result)
      if len(input_lines):
        return '\n'.join(input_lines)
      return None


parameter_create_sequence = {
  'marquee': '''
  +++ Add init parameter
  ''',
  'steps': [
    {
      'type': 'gate',
      'prompt': cli.InputPrompt('add an init parameter (Y/n)?', 'y'),
      'evaluator': lambda response: True if response.lower() == 'y' else False,

     },
    {
      'type': 'static_prompt',
      'field_name': 'value',
      'prompt': cli.InputPrompt('parameter value'),
      'required': True
    }
  ]
}


service_object_param_sequence = {
  'marquee': '''
  +++ Add init parameter to service object
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt': cli.InputPrompt('parameter name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'value',
      'prompt': cli.InputPrompt('parameter value'),
      'required': True
    }
  ]
}

service_object_create_sequence = {
  'marquee': '''
  +++
  +++ Register service object
  +++
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'alias',
      'prompt': cli.InputPrompt('service object alias'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'class',
      'prompt': cli.InputPrompt('service object class'),
      'required': True
    }
  ]
}

XFILE_FIELD_SOURCE_TYPES = [
  {'value': 'record', 'label': 'input record'},
  {'value': 'lookup', 'label': 'lookup function'},
  {'value': 'lambda', 'label': 'lambda expression'}
]

xfile_field_src_lambda = {
  'marquee': '''
  :::
  ::: set lambda-type field params
  :::
  ''',
  'inputs': {
    'source': 'lambda'
  },  
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'key',
      'prompt': cli.InputPrompt('field from source record'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'expression',
      'prompt': cli.InputPrompt('lambda expression'),
      'required': True
    },
  ]
}

xfile_field_src_record = {
  'marquee': '''
  :::
  ::: set record-type field params
  :::
  ''',
  'inputs': { 
    'source': 'record'
  },
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'key',
      'prompt': cli.InputPrompt('source-record field name'),
      'required': True
    },
  ]
}

xfile_field_src_lookup = {
  'marquee': '''
  :::
  ::: set record-type field params
  :::
  ''',
  'inputs': {
    'source': 'lookup'
  },
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'key',
      'prompt': cli.InputPrompt('lookup function (RETURN to use default)', ''),
      'required': True
    },
  ]
}

def new_xfile_fieldspec(**kwargs):
  if not kwargs.get('field_name'):
    return None
  return meta.XfileFieldSpec(kwargs['field_name'], **kwargs['field_params'])

xfile_field_create_sequence = {
  'marquee': '''
  +++ 
  +++ Add field to xfile record map
  +++
  ''',
  'builder_func': new_xfile_fieldspec,
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'field_name',
      'prompt': cli.InputPrompt('output field name'),
      'required': True
    },
    {
      'type': 'sequence_select',
      'field_name': 'field_params',
      'prompt': cli.MenuPrompt('field source:', XFILE_FIELD_SOURCE_TYPES),
      'required': True,
      'conditions': {
        'lambda': {  
          'sequence': xfile_field_src_lambda
        },
        'record': {
          'sequence': xfile_field_src_record
        },
        'lookup': {
          'sequence': xfile_field_src_lookup
        }
      } 
    }
  ]
}

xfile_map_create_sequence = {
  'marquee': '''
  +++
  +++ Create new xfile map
  +++
  ''',
  'builder_func': lambda **kwargs: meta.XfileMapSpec(kwargs['map_name'], kwargs['lookup_source']),
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'map_name',
      'prompt': cli.InputPrompt('map name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'lookup_source',
      'prompt': cli.InputPrompt('lookup datasource'),
      'required': True
    }
  ]
}

xfile_globals_create_sequence = {
  'marquee': '''
  +++ 
  +++ Create global settings group
  +++
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'project_home',
      'prompt': cli.InputPrompt('project home'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'datasource_module',
      'prompt': cli.InputPrompt('datasource module'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'service_module',
      'prompt': cli.InputPrompt('service module'),
      'required': True
    }
  ]
}

xfile_datasource_create_sequence = {
  'marquee': '''
  +++
  +++ Register datasource
  +++
  ''',
  'builder_func': lambda **kwargs: meta.DatasourceSpec(kwargs['alias'], kwargs['class']),
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'alias',
      'prompt': cli.InputPrompt('datasource alias'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'class',
      'prompt': cli.InputPrompt('datasource class'),
      'required': True 
    }
  ]
}

parameter_edit_sequence = {
  'marquee': '''
  ::: Editing parameter values
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'label': 'name',
      'field_name': 'value',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update {current_name}', '{current_value}']
    }
  ]
}


xfile_datasource_edit_sequence = {
  'marquee': '''
  ::: Editing xfile datasource
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update name', '{current_value}']
    },
    {
      'type': 'static_prompt',
      'field_name': 'classname',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update datasource class', '{current_value}']
    }
  ]
}

xfile_field_edit_sequence = {
  'marquee': '''
  ::: Editing xfile map field
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update output field name', '{current_value}']      
    },
    {
      'type': 'sequence_select',
      'field_name': 'parameters',
      'prompt': cli.MenuPrompt('field source:', XFILE_FIELD_SOURCE_TYPES),
      'required': True,
      'conditions': {
        'lambda': {  
          'sequence': xfile_field_src_lambda
        },
        'record': {
          'sequence': xfile_field_src_record
        },
        'lookup': {
          'sequence': xfile_field_src_lookup
        }
      } 
    }
  ]
}


def create_map_editor_prompt(live_config):
  menudata = []  
  for datasource in live_config.get('sources'):
    menudata.append({ 'label': datasource.name, 'value': datasource.name })  
  return cli.MenuPrompt('select lookup source', menudata)


def select_target_xfile_map_field(field_name, config_object):
  index = 0
  for fieldspec in config_object.fields:
    if fieldspec.name == field_name:
      return (fieldspec, index, xfile_field_edit_sequence)
    index += 1


xfile_map_edit_sequence = {
  'marquee': '''
  ::: Editing xfile map
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update name', '{current_value}']
    },
    {
      'type': 'dyn_prompt',
      'prompt_creator': create_map_editor_prompt,
      'field_name': 'lookup_source'      
    },
    {
      'type': 'dyn_sequence_trigger',
      'field_name': 'fields',
      'prompt_type': cli.MenuPrompt,
      'prompt_text': 'update map field',
      'selector_func': select_target_xfile_map_field
    }
  ]
}

ngst_globals_create_sequence = {
  'marquee': '''
  +++
  +++ Create ngst global settings
  +++
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'project_home',
      'prompt': cli.InputPrompt('project home'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'service_module',
      'prompt': cli.InputPrompt('service module'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'datastore_module',
      'prompt': cli.InputPrompt('datastore module'),
      'required': True
    }
  ]
}

'''
def new_ngst_datastore(**kwargs):
  dstore = meta.NgstDatastore(kwargs['alias'], kwargs['classname'])
  dstore.channel_selector_function = kwargs['channel_selector_function']
  #dstore.channels = kwargs['channels']
  return dstore
'''

ngst_datastore_create_sequence = {
  'marquee': '''
  +++
  +++ Create ngst datastore
  +++
  ''',  
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'alias',
      'prompt': cli.InputPrompt('datastore alias'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'classname',
      'prompt': cli.InputPrompt('datastore class'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'channel_selector_function',
      'prompt': cli.InputPrompt('channel-select function'),
      'required': False
    }
  ]
}


def create_ngst_datastore_prompt(live_config):
  menudata = []
  for dstore in live_config['datastores']:
    menudata.append({'label': dstore.alias, 'value': dstore.alias})
  return cli.MenuPrompt('select a datastore', menudata)


def new_ngst_target(**kwargs):
  target = meta.NgstTarget(kwargs['name'], kwargs['classname'], kwargs['checkpoint_interval'])
  return target


ngst_target_create_sequence = {

  'marquee': '''
  +++
  +++ Create ngst storage target
  +++
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt': cli.InputPrompt('target name'),
      'required': True
    },
    {
      'type': 'dyn_prompt',
      'field_name': 'datastore_alias',
      'prompt_creator': create_ngst_datastore_prompt,
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'checkpoint_interval',
      'prompt': cli.InputPrompt('checkpoint interval'),
      'required': True
    }
  ]
}


def select_datastore_init_param(param_name, config_object):
  index = 0
  for param in config_object.init_params:
    if param.name == param_name:
      return (param, index, parameter_edit_sequence)
    index += 1


ngst_datastore_edit_sequence = {
  'marquee': '''
  ::: Editing ngst datastore
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'alias',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update datastore alias', '{current_value}']
    },
    {
      'type': 'static_prompt',
      'field_name': 'classname',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update datastore class', '{current_value}']
    },
    {
      'type': 'static_prompt',
      'field_name': 'channel_selector_function',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update channel selector', '{current_value}']
    },
    {
      'type': 'sequence_selector',
      'field_name': 'init_params',
      'prompt_type': cli.MenuPrompt,
      'prompt_text': 'update init param',
      'selector_func': select_datastore_init_param,
      'default': parameter_create_sequence
    }
  ]
}


ngst_target_edit_sequence = {
  'marquee': '''
  ::: Edit ngst target
  ''',
  'steps': [
    {
      'type': 'static_prompt', 
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update target name', '{current_value}']
    },
    {
      'type': 'static_prompt',
      'field_name': 'datastore_alias',
      'prompt_type': cli.MenuPrompt,
      'prompt_text': 'update datastore alias',      
    }
  ]
}

j2sqlgen_globals_create_sequence = {
  'marquee': '''
  +++
  +++ Create j2sqlgen global settings
  +++
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'project_home',
      'prompt': cli.InputPrompt('project home'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'service_module',
      'prompt': cli.InputPrompt('service module'),
      'required': True
    }
  ]
}

autocreate_pk_options = [
  {'label': 'Yes', 'value': True},
  {'label': 'No', 'value': False}
]

j2sqlgen_defaults_create_sequence = {
  'marquee': '''
  +++
  +++ Create j2sqlgen default settings
  +++
  ''',
  'steps': [
    {
    'type': 'static_prompt',
    'field_name': 'autocreate_pk_if_missing',
    'prompt': cli.MenuPrompt('autocreate PK ?', autocreate_pk_options),
    'required': False # this is a hack
    },
    {
      'type': 'static_prompt',
      'field_name': 'pk_name',
      'prompt': cli.InputPrompt('primary key name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'pk_type',
      'prompt': cli.InputPrompt('primary key type'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'varchar_length',
      'prompt': cli.InputPrompt('VARCHAR length'),
      'required': True
    }
  ]
}

j2sqlgen_colmap_create_sequence = {
  'marquee': '''
  +++
  +++ Create j2sqlgen column type map
  +++
  ''',
  'steps': [
    {
    'type': 'static_prompt',
    'field_name': 'source_type',
    'prompt': cli.InputPrompt('source column type'),
    'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'target_type',
      'prompt': cli.InputPrompt('target column type'),
      'required': True
    }
  ]
}

j2sqlgen_table_create_sequence = {
  'marquee': '''
  +++
  +++ Create j2sqlgen table mapping
  +++
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'table_name',
      'prompt': cli.InputPrompt('table name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'new_table_name',
      'prompt': cli.InputPrompt('rename table to'),
      'required': False
    }
  ]
}


j2sqlgen_column_setting_sequence = {
  'marquee': '''
  +++
  +++ 
  +++
  '''
}

service_object_edit_sequence = {
  'marquee': '''
  ::: Editing service object
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'alias',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update alias', '{current_value}']
    },
    {
      'type': 'static_prompt',
      'field_name': 'classname',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update classname', '{current_value}']
    },
    {
      'type': 'static_sequence_trigger',
      'field_name': 'init_params',
      'sequence': parameter_edit_sequence
    }

  ]
}


quasr_globals_create_sequence = {
  'marquee': '''
  +++ 
  +++ Create QUASR global settings group
  +++
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'project_home',
      'prompt': cli.InputPrompt('project home'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'qa_logic_module',
      'prompt': cli.InputPrompt('QA logic module'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'service_module',
      'prompt': cli.InputPrompt('service module'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'template_module',
      'prompt': cli.InputPrompt('template module'),
      'required': True
    }
  ]
}

quasr_template_create_sequence = {
  'marquee': '''
  +++
  +++ Create QUASR template
  +++
  ''',
  'builder_func': lambda **kwargs: meta.QuasrTemplateSpec(kwargs['name'], kwargs['text']),
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt': cli.InputPrompt('template_name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'text',
      'prompt': MultilineInputPrompt('template text'),
      'required': True
    }
  ]
}

QUASR_SLOT_TYPES = [
  {'label': 'integer', 'value': 'int'},
  {'label': 'floating-point', 'value': 'float'},
  {'label': 'string', 'value': 'str'}
]

quasr_input_slot_create_sequence = {
  'marquee': '''
  +++
  +++ Create QUASR input slot for job template
  +++
  ''',
  'builder_func': lambda **kwargs: meta.QuasrJobIOSlot(kwargs['name'], kwargs['datatype']),
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt': cli.InputPrompt('input slot name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'datatype',
      'prompt': cli.MenuPrompt('input slot datatype', QUASR_SLOT_TYPES),
      'required': True
    }
  ]
}

quasr_output_slot_create_sequence = {
  'marquee': '''
  +++
  +++ Create QUASR output slot for job results
  +++
  ''',
  'builder_func': lambda **kwargs: meta.QuasrJobIOSlot(kwargs['name'], kwargs['datatype']),  
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt': cli.InputPrompt('output slot name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'datatype',
      'prompt': cli.MenuPrompt('output slot datatype', QUASR_SLOT_TYPES),
      'required': True
    }
  ]
}

def create_quasr_job_spec(**kwargs):
  jobspec = meta.QuasrJobSpec(kwargs['name'], kwargs['template_alias'])
  jobspec.executor_function = kwargs['executor_function']
  jobspec.builder_function = kwargs['builder_function']
  jobspec.analyzer_function = kwargs['analyzer_function']
  for slot in kwargs.get('inputs', []):
    jobspec.inputs.append(slot)
  for slot in kwargs.get('outputs', []):
    jobspec.outputs.append(slot)

  return jobspec


quasr_job_create_sequence = {
  'marquee': '''
  +++
  +++ Create QUASR job
  +++
  ''',
  'builder_func': create_quasr_job_spec,
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt': cli.InputPrompt('job name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'template_alias',
      'prompt': cli.InputPrompt('template alias'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'executor_function',
      'prompt': cli.InputPrompt('SQL executor function name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'builder_function',
      'prompt': cli.InputPrompt('output builder function name'),
      'required': True
    },
    {
      'type': 'static_prompt',
      'field_name': 'analyzer_function',
      'prompt': cli.InputPrompt('output analyzer function name'),
      'required': False
    },
    {
      'type': 'static_sequence_trigger',
      'field_name': 'inputs',
      'sequence': quasr_input_slot_create_sequence,
      'repeat': True
    },
    {
      'type': 'static_sequence_trigger',
      'field_name': 'outputs',
      'sequence': quasr_output_slot_create_sequence,
      'repeat': True
    }
  ]
}

quasr_input_slot_edit_sequence = {

  'marquee': '''
  :::
  ::: Edit QUASR input slot
  :::
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['input-slot name', '{current_value}']
    },
    {
      'type': 'static_prompt',
      'field_name': 'datatype',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['input-slot datatype', '{current_value}']
    }
  ]
}


quasr_output_slot_edit_sequence = {

  'marquee': '''
  :::
  ::: Edit QUASR output slot
  :::
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['output-slot name', '{current_value}']
    },
    {
      'type': 'static_prompt',
      'field_name': 'datatype',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['output-slot datatype', '{current_value}']
    }
  ]
}


def select_target_input_slot(slot_name, config_object):
  index = 0
  for slot in config_object.inputs:
    if slot.name == slot_name:
      return (slot, index, quasr_input_slot_edit_sequence)
    index += 1

def select_target_output_slot(slot_name, config_object):
  index = 0
  for slot in config_object.outputs:
    if slot.name == slot_name:
      return (slot, index, quasr_output_slot_edit_sequence)
    index += 1
  

quasr_job_edit_sequence = {
  'marquee': '''
  :::
  ::: Editing quasr job
  :::
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update job name', '{current_value}']
    },
    {
      'type': 'dyn_sequence_trigger', 
      'field_name': 'inputs',
      'prompt_type': cli.MenuPrompt,
      'prompt_text': 'update input slot',
      'selector_func': select_target_input_slot     
    },
    {
      'type': 'dyn_sequence_trigger',
      'field_name': 'outputs',
      'prompt_type': cli.MenuPrompt,
      'prompt_text': 'update output slot',
      'selector_func': select_target_output_slot
    }
  ]
}


quasr_template_edit_sequence = {
  'marquee': '''
  :::
  ::: Editing job template
  :::
  ''',
  'steps': [
    {
      'type': 'static_prompt',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update template name', '{current_value}'] 
    },
    {
      'type': 'static_prompt',
      'field_name': 'text',
      'prompt_type': MultilineInputPrompt,
      'prompt_args': ['update SQL template']      
    }
  ]

}


UISEQUENCE_STEP_TYPES = [
  'static_prompt',
  'dyn_prompt',
  'static_sequence_trigger',
  'dyn_sequence_trigger',
  'sequence_select'
]

class UISequenceRunner(object):
  '''
    Types of UI sequence steps (for editing):

    static_prompt: This type prompts the user for input and populates an object or field with the result

    dyn_prompt: This type prompts the user for input using a prompt created on the fly
      by a "prompt_creator" function specified in the step

    static_sequence_trigger: Launches a named UI sequence and assigns the result to the named field
        (in the case of an array-type field, the named sequence will run once per element)

    dyn_sequence_trigger: This type is designed to handle collection-type object fields in situations
        where the user wants to edit only one member of the collection. It prompts the user to 
        choose from an array of options, then passes the result to a function which returns
        the target UISequence.

    dyn_sequence_passthrough: 

    gate: this type prompts the user for input, evaluates that input as true or false, and triggers
        a named sequence if the result evaluated to True.

    sequence_select: Displays a MenuPrompt and, based on the response, triggers one of N sequences
        defined in the step's "conditions" attribute. 
  '''

  def __init__(self, **kwargs):
    #
    # keyword args:
    # create_prompts is an optional dictionary
    # where <key> is the field name of a step in a UI sequence
    # and <value> is a prompt instance from the cli library.
    # This gives users of the UISequenceRunner the option to 
    # override the default Prompt type specified in the ui sequence 
    # dictionary passed to the create() method.
    #
    
    self.create_prompts = kwargs.get('create_prompts') or {}
    self.edit_menus = kwargs.get('edit_menus', {})
    self.configuration = kwargs.get('configuration', {})
    

  def process_edit_sequence(self, config_object, **sequence):
    
    if not config_object:
      return
    print(sequence['marquee'])
    context = {}
    for step in sequence['steps']:

      step_type = step.get('type')
      current_target = getattr(config_object, step['field_name'])
      context['current_value'] = getattr(config_object, step['field_name'])
      label = step.get('label', step['field_name'])
      context['current_name'] = getattr(config_object, label)
            
      if not step_type in UISEQUENCE_STEP_TYPES:
        raise Exception('The step with field name "%s" is of an unsupported type "%s".' % (step['field_name'], step_type))

      if step_type == 'static_sequence_trigger':
        if isinstance(current_target, list):
          # recursively edit embedded lists 
          response = []
          for obj in current_target:
            output = self.process_edit_sequence(obj, **step['sequence'])
            if output is not None:
              response.append(output)
              setattr(config_object, step['field_name'], response)
          
        # directly execute a named sequence
        else:
          output = self.process_edit_sequence(**step['sequence'])
          if output is not None:
            setattr(config_object, step['field_name'], output)        

      elif step_type == 'sequence_select':
        prompt = step['prompt']
        selection = prompt.show()
        if not step['conditions'].get(selection):
          raise Exception('No condition in step conditions matching selection "%s".' % selection)

        next_sequence = step['conditions'][selection]['sequence']
        #print(next_sequence)
        output = self.process_edit_sequence(config_object, **next_sequence)        
        if output is not None:
          config_object = output

      # execute one of N sequences depending on user input
      elif step_type == 'dyn_sequence_trigger':
        if not 'selector_func' in step.keys():
          raise Exception('a step of type "dyn_sequence_trigger" must specify a selector_func field.')
        
        prompt = step['prompt_type']
        selector_func = step['selector_func']
        menudata = self.edit_menus.get(step['field_name'])
        if menudata is None:
          menudata = step.get('menu_data')
        if menudata is None:        
          raise Exception('a step of type "dyn_sequence_trigger" must have a menu_data field OR pass it in the UISequence constructor.')

        # skip if there are no entries to select
        if not len(menudata):
          continue

        prompt_text = step['prompt_text'].format(**context)
        args = [prompt_text, menudata]
        
        # retrieve user input
        selection = prompt(*args).show()
        
        if not selection:
          continue
  
        # dynamic dispatch; use user input to determine the next sequence
        # (selector_func() MUST return a live UISequence dictionary)
        target_object, object_index, next_sequence = selector_func(selection, config_object)
        if not target_object:
          continue
        updated_object = self.edit(target_object, **next_sequence)
        if updated_object:
          if isinstance(getattr(config_object, step['field_name']), list):
            # replace the list element
            obj_list = getattr(config_object, step['field_name'])
            obj_list[object_index] = updated_object
            setattr(config_object, step['field_name'], obj_list)
          else:
            # just replace the object
            setattr(config_object, step['field_name'], updated_object)
        
      elif step_type == 'static_prompt':
        prompt = step['prompt_type']

        args = []
        if step.get('prompt_args'):
          for a in step['prompt_args']:
            args.append(a.format(**context))
        
        elif step.get('prompt_text'):          
          args.append(step['prompt_text'])
          menudata = self.edit_menus.get(step['field_name']) or step.get('menu_data')
          if not menudata:
            raise Exception('A "static_prompt" type ui step must provide its own menu data or retrieve it from the UISequenceRunner.')
          args.append(menudata)
        
        # just execute this step
        response = prompt(*args).show()
        if response is not None:
          setattr(config_object, step['field_name'], response)

      elif step_type == 'dyn_prompt':
        prompt_create_func = step['prompt_creator']
        answer = prompt_create_func(self.configuration).show()
        setattr(config_object, step['field_name'], answer) 

    return config_object


  def process_create_sequence(self, init_context=None, **sequence):
    print(sequence['marquee'])
    context = {}

    if init_context:
      context.update(init_context)
    
    if sequence.get('inputs'):
      context.update(sequence['inputs'])

    for step in sequence['steps']:     
      step_type = step['type']
      if step_type not in UISEQUENCE_STEP_TYPES:
        raise Exception('found a UISequence step of an unsupported type [%s].' % step_type)
      
      if step_type == 'gate':
        '''fields: type, prompt, evaluator'''
        print('placeholder for gate-type sequence step')

      elif step_type == 'sequence_select':
        prompt = step['prompt']
        user_choice = prompt.show()
        if not user_choice:
          return None

        if not step['conditions'].get(user_choice):
          raise Exception('No sequence registered in this step for user selection "%s".' % user_choice)
        
        next_sequence = step['conditions'][user_choice]['sequence']        
        sequence_output = self.create(**next_sequence)
        if sequence_output:
          context[step['field_name']] = sequence_output

      elif step_type == 'dyn_prompt':
        prompt_create_func = step['prompt_creator']
        answer = prompt_create_func(self.configuration).show()
        context[step['field_name']] = answer        
      
      elif step_type == 'static_prompt':        
        # follow the prompt -- but override the one in the UI sequence if one was passed to us
        # in our constructor
        prompt =  self.create_prompts.get(step['field_name'], step['prompt'])         
        answer = prompt.show()
        if not answer and step['required'] == True:
          return None
        if not answer and hasattr(step, 'default'):
          answer = step['default']
        
        context[step['field_name']] = answer        

      elif step_type == 'static_sequence_trigger':
        next_sequence = step['sequence']        
        is_repeating_step =  step.get('repeat', False)

        while True:                 
          sequence_output = self.create(**next_sequence)
          if not sequence_output: 
            break

          if is_repeating_step:
            if not context.get(step['field_name']):
              context[step['field_name']] = []
            context[step['field_name']].append(sequence_output)
          else:
            context[step['field_name']] = sequence_output

          if is_repeating_step:
            repeat_prompt = step.get('repeat_prompt', cli.InputPrompt('create another (Y/n)', 'y'))
            should_repeat = repeat_prompt.show().lower()
            if should_repeat == 'n':
              break
          else:
            break

      elif step_type == 'dyn_sequence_trigger':
        if not 'selector_func' in step.keys():
          raise Exception('a step of type "dyn_sequence_trigger" must specify a selector_func field.')

        prompt = step['prompt_type']
        selector_func = step['selector_func']
        menudata = self.edit_menus.get(step['field_name']) or step.get('menu_data')

        if menudata is None:        
          raise Exception('a step of type "dyn_sequence_trigger" must have a menu_data field OR pass it in the UISequence constructor.')

        # skip if there are no entries to select
        if not len(menudata):
          continue

        prompt_text = step['prompt_text'].format(**context)
        args = [prompt_text, menudata]
        
        # retrieve user input
        selection = prompt(*args).show()
        
        if not selection:
          continue
  
        # dynamic dispatch; use user input to determine the next sequence
        # (selector_func() MUST return a live UISequence dictionary)
        next_sequence = selector_func(selection)
        if not next_sequence:
          continue

        sequence_output = self.create(**next_sequence)
        if sequence_output:
          context[step['field_name']] = sequence_output
        
    return context


  def create(self, **create_sequence):
    #print(create_sequence)
    context = self.process_create_sequence(**create_sequence)
    output_builder = create_sequence.get('builder_func')
    if context and output_builder:
      return output_builder(**context)
    return context


  def edit(self, config_object, **edit_sequence):
    self.process_edit_sequence(config_object, **edit_sequence)
    return config_object
