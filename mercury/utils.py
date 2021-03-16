#!/usr/bin/env python

import os, sys
from os import system, name
import itertools
from enum import Enum
import re
from contextlib import contextmanager
from snap import common


MACRO_RX = re.compile(r'^~macro\[.+\]$')
MACRO_PREFIX = '~macro['
MACRO_SUFFIX = ']'

TEMPLATE_RX = re.compile(r'^~template\[.+\]$')
TEMPLATE_PREFIX = '~template['
TEMPLATE_SUFFIX = ']'

TEMPLATE_ENV_VAR_RX = re.compile(r'\$\{.+?\}')


class Whitespace(Enum):
  space = ' '
  tab = '\t'

def tab(num_tabs):
  return ''.join(itertools.repeat('\t', num_tabs))
  
def space(num_spaces):
  return ''.join(itertools.repeat(' ', num_spaces))

def indent(num_indents, whitespace_type):
  return ''.join(itertools.repeat(whitespace_type, num_indents))

def clear(): 
    # for windows 
    if name == 'nt': 
        _ = system('cls') 

    # for mac and linux(here, os.name is 'posix') 
    else: 
        _ = system('clear') 


def read_stdin():
    for line in sys.stdin:
        if sys.hexversion < 0x03000000:
            line = line.decode('utf-8')
        yield line.lstrip().rstrip()


@contextmanager
def open_in_place(filename: str, open_mode: str):
    if filename.startswith(os.path.sep):
        filepath = filename
    else:
        filepath = os.path.join(os.getcwd(), filename)

    with open(filepath, open_mode) as f:
        yield f


def parse_cli_params(params_array):
    data = {}
    if len(params_array):
        params_string = params_array[0]
        nvpair_tokens = params_string.split(',')
        for nvpair in nvpair_tokens:
            if ':' not in nvpair:
                raise Exception('parameters passed to warp must be in the format <name:value>.')

            tokens = nvpair.split(':') 
            key = tokens[0]
            value = tokens[1]
            data[key] = value

    return data

def eval_macro(macro_funcname, yaml_config, **kwargs):

    macro_module = yaml_config['globals'].get('macro_module')
    if not macro_module:
        raise Exception('you have defined a macro, but there is no macro_module in the globals section. Please check your config file.')

    macro_func = None
    try:
        macro_func = common.load_class(macro_funcname, macro_module)        
    except AttributeError:
        raise Exception(f'The code macro "{macro_funcname}" was not found in the specified macro_module {macro_module}.')

    return macro_func()



def resolve_env_vars_in_template(template_string):
    # now find refs to environment variables. WITHIN a ~template[] block, they
    # are formatted like this:
    #
    # ${ENV_VAR}
    #
    result = template_string
    for match_obj in re.finditer(TEMPLATE_ENV_VAR_RX, template_string):
        str_match = template_string[match_obj.start():match_obj.end()]
        var_name = str_match.lstrip('${').rstrip('}')
        var_value = os.getenv(var_name)
        if not var_value:
            raise Exception(f'The environment variable {var_name} was referenced in a template expression, but has not been set.')

        result = result.replace(str_match, var_value)

    return result


def load_config_dictionary(config_dict, yaml_config):
    data = {}
    if not config_dict:
        return data

    for key, value in config_dict.items():
        
        macro_rx_match = MACRO_RX.match(str(value))
        template_rx_match = TEMPLATE_RX.match(str(value))

        # if we have a macro as the value, load and execute it
        if macro_rx_match:
            macro_function = str(value).lstrip(MACRO_PREFIX).rstrip(MACRO_SUFFIX)                
            data[key] = eval_macro(macro_function, yaml_config)

        # if we have a template as the value, populate it
        elif template_rx_match:
            template_str = str(value).lstrip(TEMPLATE_PREFIX).rstrip(TEMPLATE_SUFFIX)
            
            #print(f'####### found template string: {template_str}')
            
            new_value = resolve_env_vars_in_template(template_str)
            #print(f'@@@@@@@   Resolved template string {template_str} to value {new_value}')

            data[key] = new_value

        # if the value is not a macro or a template, see if it's an env var
        else:
            data[key] = common.load_config_var(value)

    return data
