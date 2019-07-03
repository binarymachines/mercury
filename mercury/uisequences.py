#!/usr/bin/env python

import copy
from snap import cli_tools as cli
from snap import common
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
      'type': 'direct',
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
      'type': 'direct',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update name', '{current_value}']
    },
    {
      'type': 'direct',
      'field_name': 'lookup_source',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update lookup source', '{current_value}']
    },
    {
      'type': 'static_sequence_trigger',
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
      'type': 'direct',
      'field_name': 'alias',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update alias', '{current_value}']
    },
    {
      'type': 'direct',
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

quasr_input_slot_edit_sequence = {

  'marquee': '''
  :::
  ::: Edit QUASR input slot
  :::
  ''',
  'steps': [
    {
      'type': 'direct',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['input-slot name', '{current_value}']
    },
    {
      'type': 'direct',
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
      'type': 'direct',
      'field_name': 'name',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['output-slot name', '{current_value}']
    },
    {
      'type': 'direct',
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
  ''',
  'steps': [
    {
      'type': 'direct',
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
    self.edit_menus = kwargs.get('edit_menus', {})
    self.allowed_edit_step_types = set(['direct',
                                       'dyn_sequence_trigger',
                                       'static_sequence_trigger'])

  def process_edit_sequence(self, config_object, **sequence):
    
    print(sequence['marquee'])
    context = {}
    for step in sequence['steps']:

      step_type = step.get('type')

      current_target = getattr(config_object, step['field_name'])
      print('### pulling menu data for step with field name %s..' % step['field_name'])

      context['current_value'] = getattr(config_object, step['field_name'])
      label = step.get('label', step['field_name'])
      context['current_name'] = getattr(config_object, label)
      
      '''
      args = []
      for a in step['prompt_args']:
        args.append(a.format(**context))
      '''

      # fanout: execute a child sequence once for each element in a list
      #if isinstance(current_target, list) and step.get('sequence'):
      if not step_type in self.allowed_edit_step_types:
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
        continue

      # execute one of N sequences depending on user input
      if step_type == 'dyn_sequence_trigger':
        if not 'selector_func' in step.keys():
          raise Exception('a step of type "dyn_sequence_trigger" must specify a selector_func field.')
        
        prompt = step['prompt_type']
        selector_func = step['selector_func']
        menudata = step.get('menu_data') or self.edit_menus.get(step['field_name'])
        if not menudata:
          raise Exception('a step of type "dyn_sequence_trigger" must have a menu_data field OR provide it in the context.')

        prompt_text = step['prompt_text'].format(**context)
        args = [prompt_text, menudata]
        
        # retrieve user input
        selection = prompt(*args).show()
        # dynamic dispatch; use user input to determine the next sequence
        # (selector_func() MUST return a live UISequence dictionary)
        if not selection:
          continue

        print('### selected config object: %s' % config_object)  
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
        continue

      if step_type == 'direct':
        prompt = step['prompt_type']
        args = []
        for a in step['prompt_args']:
          args.append(a.format(**context))
        # just execute this step
        response = prompt(*args).show()
        if response is not None:
          setattr(config_object, step['field_name'], response)

      '''
      
      if 'sequence' in step.keys():
        output = self.process_edit_sequence(**step['sequence'])
        if output:
          setattr(config_object, step['field_name'], output)
        continue
      ''' 

      
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
        answer = step['prompt'].show()
        if not step['conditions'].get(answer):
          raise Exception('a step "%s" in the UI sequence returned an answer "%s" for which there is no condition.'
                          % (step['field_name'], answer))

        next_sequence = step['conditions'][answer]['sequence']
        outgoing_context = copy.deepcopy(context)
        context[step['field_name']] = self.create(**next_sequence)
         
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

    '''
    {
      'field_name': 'template_alias',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update template alias', '{current_value}']
    },
    {
      'field_name': 'executor_function',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update executor funcname', '{current_value}']
    },
    {
      'field_name': 'builder_function',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update builder funcname', '{current_value}']
    },
    {
      'field_name': 'analyzer_function',
      'prompt_type': cli.InputPrompt,
      'prompt_args': ['update analyzer funcname', '{current_value}']
    },
    '''