#!/usr/bin/env python

import copy
from snap import cli_tools as cli
from mercury import metaobjects as meta


service_object_param_sequence = {
  'marquee': '''
  +++ Add init parameter to service object
  ''',
  'steps': [
    {
      'field_name': 'name',
      'prompt': cli.InputPrompt('parameter name'),
      'required': True
    },
    {
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
      'field_name': 'alias',
      'prompt': cli.InputPrompt('service object alias'),
      'required': True
    },
    {
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
      'field_name': 'key',
      'prompt': cli.InputPrompt('field from source record'),
      'required': True
    },
    {
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
      'field_name': 'field_name',
      'prompt': cli.InputPrompt('output field name'),
      'required': True
    },
    {
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
      'field_name': 'map_name',
      'prompt': cli.InputPrompt('map name'),
      'required': True
    },
    {
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
      'field_name': 'project_home',
      'prompt': cli.InputPrompt('project home'),
      'required': True
    },
    {
      'field_name': 'datasource_module',
      'prompt': cli.InputPrompt('datasource module'),
      'required': True
    },
    {
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
  'steps': [
    {
      'field_name': 'alias',
      'prompt': cli.InputPrompt('datasource alias'),
      'required': True
    },
    {
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
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update name', '{current_value}']
    },
    {
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
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update output field name', '{current_value}']      
    },
    {
      'field_name': 'parameters',
      'sequence': parameter_edit_sequence
    }
  ]
}

xfile_map_edit_sequence = {
  'marquee': '''
  ::: Editing xfile map
  ''',
  'steps': [
    {
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update name', '{current_value}']
    },
    {
      'field_name': 'lookup_source',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update lookup source', '{current_value}']
    },
    {
      'field_name': 'fields',
      'sequence': xfile_field_edit_sequence
    }
  ]
}


service_object_edit_sequence = {
  'marquee': '''
  ::: Editing service object
  ''',
  'steps': [
    {
      'field_name': 'alias',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update alias', '{current_value}']
    },
    {
      'field_name': 'classname',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update classname', '{current_value}']
    },
    {
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
      'field_name': 'project_home',
      'prompt': cli.InputPrompt('project home'),
      'required': True
    },
    {
      'field_name': 'qa_logic_module',
      'prompt': cli.InputPrompt('QA logic module'),
      'required': True
    },
    {
      'field_name': 'service_module',
      'prompt': cli.InputPrompt('service module'),
      'required': True
    },
    {
      'field_name': 'template_module',
      'prompt': cli.InputPrompt('template module'),
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
      'field_name': 'name',
      'prompt': cli.InputPrompt('input slot name'),
      'required': True
    },
    {
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
      'field_name': 'name',
      'prompt': cli.InputPrompt('output slot name'),
      'required': True
    },
    {
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
  for slot in kwargs['inputs']:
    jobspec.inputs.append(slot)
  for slot in kwargs['outputs']:
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
      'field_name': 'name',
      'prompt': cli.InputPrompt('job name'),
      'required': True
    },
    {
      'field_name': 'template_alias',
      'prompt': cli.InputPrompt('template alias'),
      'required': True
    },
    {
      'field_name': 'executor_function',
      'prompt': cli.InputPrompt('SQL executor function name'),
      'required': True
    },
    {
      'field_name': 'builder_function',
      'prompt': cli.InputPrompt('output builder function name'),
      'required': True
    },
    {
      'field_name': 'analyzer_function',
      'prompt': cli.InputPrompt('output analyzer function name'),
      'required': False
    },
    {
      'field_name': 'inputs',
      'sequence': quasr_input_slot_create_sequence,
      'repeat': True
    },
    {
      'field_name': 'outputs',
      'sequence': quasr_output_slot_create_sequence,
      'repeat': True
    }
  ]
}


class UISequenceRunner(object):
  def __init__(self, **kwargs):
    #
    # keyword args:
    # override_create_prompts is an optional dictionary
    # where <key> is the field name of a step in a UI sequence
    # and <value> is a prompt instance from the cli library.
    # This gives users of the UISequenceRunner the option to 
    # override the default Prompt type specified in the ui sequence 
    # dictionary passed to the create() method.
    #
    
    self.create_prompts = kwargs.get('override_create_prompts', {})


  def process_edit_sequence(self, config_object, **sequence):
    
    print(sequence['marquee'])
    context = {}
    for step in sequence['steps']:

      current_target = getattr(config_object, step['field_name'])
      if step.get('sequence'):
        if isinstance(current_target, list):
          # recursively edit embedded lists 
          response = []
          for obj in current_target:
            output = self.process_edit_sequence(obj, **step['sequence'])
            if output is not None:
              response.append(output)

          setattr(config_object, step['field_name'], response)
          continue

        else:
          output = self.process_edit_sequence(**step['sequence'])
          if output:
            setattr(config_object, step['field_name'], output)
          continue
      else:
        if isinstance(current_target, list):
          raise Exception('!!! An edit sequence which handles a list-type attribute must use a child sequence.')   

      context['current_value'] = getattr(config_object, step['field_name'])
      label = step.get('label', step['field_name'])
      context['current_name'] = getattr(config_object, label)      
      prompt = step['prompt_type']
      args = []
      for a in step['prompt_args']:
        args.append(a.format(**context))
      response = prompt(*args).show()
      if response is not None:
        setattr(config_object, step['field_name'], response)
    return config_object


  def process_create_sequence(self, init_context=None, **sequence):
    print(sequence['marquee'])
    context = {}
    if init_context:
      context.update(init_context)
    
    if sequence.get('inputs'):
      context.update(sequence['inputs'])

    for step in sequence['steps']:

      if not step.get('prompt'):
        if not step.get('conditions') and not step.get('sequence'):
          # hard error
          raise Exception('step "%s" in this UI sequence has no prompt and does not branch to a child sequence') 

      # this is an input-dependent branch 
      if step.get('conditions'):
        if not step['conditions'].get(answer):
          raise Exception('a step "%s" in the UI sequence returned an answer "%s" for which there is no condition.' 
                          % (step['field_name'], answer))

        next_sequence = step['conditions'][answer]['sequence']
        outgoing_context = copy.deepcopy(context)
        context[step['field_name']] = self.process_create_sequence(**next_sequence)
         
      # unconditional branch
      elif step.get('sequence'):
        next_sequence = step['sequence']
        outgoing_context = copy.deepcopy(context)
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

      else:
        # follow the prompt -- but override the one in the UI sequence if one was passed to us
        # in our constructor
        prompt =  self.create_prompts.get(step['field_name'], step['prompt'])         
        answer = prompt.show()
        if not answer and step['required'] == True:        
          break
        if not answer and hasattr(step, 'default'):        
          answer = step['default']
        else:        
          context[step['field_name']] = answer
  
    return context


  def create(self, **create_sequence):
    context = self.process_create_sequence(**create_sequence)
    output_builder = create_sequence.get('builder_func')
    if output_builder:
      return output_builder(**context)
    return context


  def edit(self, config_object, **edit_sequence):
    self.process_edit_sequence(config_object, **edit_sequence)
    return config_object
