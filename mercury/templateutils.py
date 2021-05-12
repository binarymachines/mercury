#!/usr/bin/env python

import os, sys
import re
import typing
from abc import ABC, abstractmethod
from typing import Callable
from snap import common


MENU_RX = re.compile(r'^~menu\[.+\]$')
MENU_PREFIX = '~menu['
MENU_SUFFIX = ']'

MACRO_RX = re.compile(r'~macro\[[a-zA-Z-_.]+\]')
MACRO_PREFIX_RX = re.compile(r'~macro\[')
MACRO_SUFFIX_RX = re.compile(r'\]')

TEMPLATE_RX = re.compile(r'~template\[.+\]')
TEMPLATE_PREFIX_RX = re.compile(r'~template\[')
TEMPLATE_SUFFIX_RX = re.compile(r'\]')

TEMPLATE_ENV_VAR_RX = re.compile(r'\$\(.+?\)')
TEMPLATE_ENV_VAR_PREFIX = '$('
TEMPLATE_ENV_VAR_SUFFIX = ')'


class UnregisteredMacroError(Exception):
    def __init__(self, macro_name):
        super().__init__(f'An unregistered macro "{macro_name}" is being invoked in an API target. Please check your configuration.')


class MissingMacroArgError(Exception):
    def __init__(self, argname, macro_definition):
        super().__init__(f'''
            The macro "{macro_definition.name}" is being invoked, but the argument "{argname}" is missing.
            Required arguments are: {macro_definition.arg_names}.''')

class AmbiguousMacroReference(Exception):
    def __init__(self, macro_module_name: str, dotted_macro_reference: str):
        super().__init__(f'''Ambiguous macro reference: the macro module "{macro_module_name}" was specified,
                        but the macro function ref "{dotted_macro_reference}" is of the form
                        <macro_module>.<macro_function>.
                        You may use one or the other, but not both.''')
          

class MacroDefinition(object):
    def __init__(self, name, *arg_names):
        self.name = name
        self.arg_names = arg_names


class Macro(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def evaluate(self, **kwargs):
        pass

    def required_args(self) -> list:
        return []

    def verify_args(self, **kwargs):          
        arg_names = self.required_args()
        for arg in arg_names:
            if arg not in kwargs.keys():
                raise MissingMacroArgError(arg, MacroDefinition(self.__class__.__name__, *arg_names))

    def call(self, **kwargs) -> str:
        self.verify_args(**kwargs)
        return self.evaluate(**kwargs)


class LoadableMacro(Macro):
    def __init__(self, handler_function: Callable, *args):
        self._required_args = args
        self._handler_function = handler_function
    
    def required_args(self):
        return self._required_args

    def evaluate(self, **kwargs):
        return self._handler_function(**kwargs)


def template_contains_macros(template_string):    
    if not MACRO_RX.search(str(template_string)):
        return False
    return True


def strip_tags(prefix_rx, suffix_rx, input_string):
    
    prefix_match = re.search(prefix_rx, input_string)
    suffix_match = re.search(suffix_rx, input_string)
    
    prefix_offset = prefix_match.span()[0]
    prefix_extent = prefix_match.span()[1]

    suffix_offset = suffix_match.span()[0]
    suffix_extent = suffix_match.span()[1]

    return input_string[prefix_offset + prefix_extent:suffix_offset-suffix_extent]


def resolve_macros_in_template(input_string, macro_module_name, **kwargs):
    macro_args = {}
    macro_args.update(kwargs)

    macro_module = None
    if macro_module_name:
        macro_module = __import__(macro_module_name)

    else:
        print(f'NOTE: No explicit macro module was specified, so any macro refs must be in the format <module.name>.',
              file=sys.stderr)

        return input_string

    for macro_match in re.finditer(MACRO_RX, str(input_string)):
        macro_expression = macro_match.group()

        macro_ref = strip_tags(MACRO_PREFIX_RX, MACRO_SUFFIX_RX, macro_expression)

        if '.' in macro_ref:
            
            macro_exp_tokens = macro_ref.split('.')
            if len(macro_exp_tokens) > 2:
                raise Exception(f'Bad macro syntax: {macro_expression}.\nA macro reference must either be of the form "~macro[macroname]" or "~macro[module.macroname".')

            if len(macro_exp_tokens) == 2:
                macro_module_name = macro_exp_tokens[0]
                macro_name = macro_exp_tokens[1]
                macro_module = __import__(macro_module_name)
   
        else:
            macro_name = macro_ref

        if not macro_module:
            raise Exception(f'Macro reference detected in template, but no macro module was specified. Etiher name one explicitly, or use dot notation in the macro expression.')
        try:
            macro_obj = getattr(macro_module, macro_name)
        except AttributeError:
            raise Exception(f'The macro "{macro_name}" was not found in the macros module "{macro_module.__name__}".')

        if isinstance(macro_obj, Macro):
            macro_instance = macro_obj()
        elif macro_obj.__class__ == typing.types.FunctionType:
            macro_instance = LoadableMacro(macro_obj)
        else:
            raise Exception(f'Unsupported macro type "{macro_obj.__class__.__name__}".')
        
        input_string = input_string.replace(macro_expression, str(macro_instance.call(**macro_args)))

    return input_string


def eval_macro(macro_funcname, macro_module, **kwargs):

    macro_func = None
    try:
        macro_func = common.load_class(macro_funcname, macro_module)        
    except AttributeError:
        raise Exception(f'The macro function "{macro_funcname}" was not found in the specified macro_module {macro_module}.')

    return macro_func(**kwargs)


def resolve_env_vars_in_template(template_string):
    # now find refs to environment variables. WITHIN a ~template[] block, they
    # are formatted like this:
    #
    # ${ENV_VAR}
    #
    result = template_string
    for match_obj in re.finditer(TEMPLATE_ENV_VAR_RX, template_string):
        str_match = template_string[match_obj.start():match_obj.end()]

        var_name = str_match.lstrip(TEMPLATE_ENV_VAR_PREFIX).rstrip(TEMPLATE_ENV_VAR_SUFFIX)
        var_value = os.getenv(var_name)
        if not var_value:
            raise Exception(f'The environment variable {var_name} was referenced in a template expression, but has not been set.')

        result = result.replace(str_match, var_value)

    return result


def process_template_input_params(param_dict, macro_module_name=None, macro_args_dict={}):
    data = {}
    if not param_dict:
        return data

    for key, value in param_dict.items():
        
        macro_rx_match = MACRO_RX.match(str(value))
        template_rx_match = TEMPLATE_RX.match(str(value))

        # if we have a macro as the value, load and execute it
        if macro_rx_match:
            macro_function_name = strip_tags(MACRO_PREFIX_RX, MACRO_SUFFIX_RX, macro_rx_match.group())

            if '.' in macro_function_name:
                #
                # Some of the Mercury template-aware tools (such as beekeeper) allow the user to give a macro module name, 
                # either at the command line, or in the config file. In the case of warp, when it's running in 
                # explicit-command mode (no configuration file specified), 
                # we can also use "dot notation" to specify a module that we want to try to load a macro from.
                # 
                # If a template input parameter contains a macro reference which itself contains a dot:
                # 
                #   --params=<name>:~macro[modname.funcname]
                #
                # then Warp will interpret the macro reference as "invoke the macro function 'funcname'
                # from Python module 'modname' ".
                #
                # If we have been passed an explicit macro module name AND the macro reference we just parsed
                # contains a dot, then the module name passed WITHIN THE MACRO REFERENCE will take precedence.
                #
                # Note: This decision is subject to change. I'm attempting to balance ease-of-use against subtlety, 
                # and I may decide to disallow ambiguous macro refs for the sake of eliminating subtlety. --DT
                #

                # resolve the macro reference using dot notation
                macro_tokens = macro_function_name.split('.')
                macro_module_name = macro_tokens[0]
                macro_function_name = macro_tokens[1]

            # 
            # If the macro reference contains no dot, then Warp will (politely) error out if the macro module name
            # has not been explicitly specified.
            #
            else:
                if not macro_module_name:
                    raise Exception('Template input params specify a macro, but no macro module name was supplied. Please check your command line.')
 
            data[key] = eval_macro(macro_function_name, macro_module_name, **macro_args_dict)

        # if we have a template as the value, populate it
        elif template_rx_match:
            template_str = strip_tags(TEMPLATE_PREFIX_RX, TEMPLATE_SUFFIX_RX, template_rx_match.group())            
            new_value = resolve_env_vars_in_template(template_str)
            data[key] = new_value

        # if the value is not a macro or a template, see if it's an env var
        else:
            data[key] = common.load_config_var(value)

    return data


def resolve_embedded_expressions(template_string: str, macro_module_name: str=None, **macro_args):
    
    # resolve macros
    output_str = resolve_macros_in_template(template_string, macro_module_name, **macro_args)

    # take care of any environment vars
    output_str = resolve_env_vars_in_template(output_str)
    return output_str
