#!/usr/bin/env python

'''
Usage:
    quasr [-p] --config <configfile> --job <jobname> [--params=<name:value>...]
    quasr [-p] --config <configfile> --jobs [--file <filename>]
    quasr --config <configfile> --list [-v]

Options:
    -p --preview      preview mode
    -v --verbose      verbose job listing
'''

# QUASR: Quality Assurance SQL Runner

import os, sys
import json
import re
import docopt
from collections import namedtuple
from snap import snap, common


PARAM_DELIMITER = ':'
PARAM_TOKEN_DELIMITER = ','

integer_rx = re.compile(r'^[0-9]+$')
float_rx = re.compile(r'^[0-9]*.[0-9]+$')
string_rx = re.compile(r'^[a-zA-Z0-9_]+$')


TypeFormat = namedtuple('TypeFormat', 'class_object regex')

type_formats = {
    'str': TypeFormat(class_object=str, regex=string_rx),
    'float': TypeFormat(class_object=float, regex=float_rx),
    'int': TypeFormat(class_object=int, regex=integer_rx)
}

Slot = namedtuple('Slot', 'name datatype')

class QaSqlNode(object):
    def __init__(self, query_template):
        self.sql_template = query_template
        self.input_slots = {}
        self.output_slots = {}


    def add_input_slot(self, name, classname):
        self.input_slots[name] = Slot(name, classname)


    def add_output_slot(self, name, classname):
        self.output_slots[name] = Slot(name, classname)


    def validate_input_types(self, **kwargs):
        for name, _ in self.input_slots.items():
            if kwargs.get(name) is None:
                raise Exception('No data supplied for input slot "%s".' % name)

        for name, value in kwargs.items():
            input_slot = self.input_slots.get(name)
            if not input_slot:
                raise Exception('No input slot "%s" registered with this QA node.')

            if value.__class__.__name__ != input_slot.datatype:
                raise Exception('The input parameter "%s" does not match the registered slot type "%s".' 
                                % (name, input_slot.datatype))

    def validate_outputs(self, **kwargs):
        for slot_name, output_slot in self.output_slots.items():
            if not kwargs.get(slot_name):
                raise Exception('This QA job node contains an output slot "%s", but no such output field was generated.' % slot_name)
            

    def generate_query(self, **kwargs):
        kwreader = common.KeywordArgReader(*self.input_slots.keys())
        kwreader.read(**kwargs)
        self.validate_input_types(**kwargs)
        return self.sql_template.format(**kwargs)


class QaJobRunner(object):
    def __init__(self, yaml_config):
        self.qa_nodes = {}
        self.config = yaml_config
        self.service_registry = common.ServiceObjectRegistry(snap.initialize_services(yaml_config))
        self.analyzers = {}

        job_config_group = yaml_config['jobs']
        for nodename in job_config_group:
            template = self._load_template(job_config_group[nodename]['sql_template'])
            inputs =  self._load_input_values(job_config_group[nodename]['inputs'] or [])
            outputs = self._load_output_values(job_config_group[nodename]['outputs'] or [])

            if job_config_group[nodename].get('analyzer'):
                logic_module_name = self.config['globals']['qa_logic_module']
                analyzer_func = common.load_class(job_config_group[nodename].get('analyzer'),
                                                  logic_module_name)
                self.analyzers[nodename] = analyzer_func                                                  
            node = QaSqlNode(template)

            for input in inputs:                
                node.add_input_slot(input['name'], input['type'])

            for output in outputs:                
                node.add_output_slot(output['name'], output['type'])

            self.qa_nodes[nodename] = node

    def _lookup_analyzer(self, jobname):
        return self.analyzers.get(jobname)


    def _load_input_values(self, dict_array):
        input_values = []
        for input in dict_array:
            input_values.append({'name': input['name'], 'type': input['type']})
        return input_values


    def _load_output_values(self, dict_array):
        output_values = []
        for output in dict_array:
             output_values.append({'name': output['name'], 'type': output['type']})
        return output_values


    def _load_template(self, template_name):        
        if template_name.startswith('(') and template_name.endswith(')'):
            # a template name in parentheses means that the template itself 
            # is in the config file
            template_name = template_name.lstrip('(').rstrip(')')
            if not self.config.get('templates'):
                raise Exception('bad internal template ref: the configfile has no "templates" section.')
            if not self.config['templates'].get(template_name):
                raise Exception('no template registered under the alias "%s" in the "templates" config section.'
                                % template_name)
            return self.config['templates'][template_name]
        else:
            # otherwise the template is in the module referenced in the 
            # globals section as template_module
            
            # this is not really loading a class -- the underlying logic just pulls the named
            # attribute of the loaded Python module using getattr(). 
            # TODO: create a load_module_object() method, so as not to mislead
            return common.load_class(template_name, self.config['globals']['template_module'])
        

    def _lookup_node(self, job_name):
        node = self.qa_nodes.get(job_name)
        if not node:
            raise Exception('no job registered under alias "%s".' % job_name)
        return node


    def get_sql_template(self, job_name):
        node = self._lookup_node(job_name)        
        return node.sql_template


    def inputs(self, job_name):
        node = self._lookup_node(job_name)
        result = {}
        for name, slot in node.input_slots.items():
            result[name] = slot.datatype
        return result


    def convert_input_params(self, job_name, **kwargs):
        node = self._lookup_node(job_name)
        job_params = {}
        slots = node.input_slots
        for param_name, param in kwargs.items():
            input_slot = slots.get(param_name)
            if not input_slot:
                raise Exception('no input slot registered with name "%s".' % param_name)
            
            type_format = type_formats.get(input_slot.datatype)
            if not type_format:
                raise Exception('input datatype "%s" is not supported.' % input_slot.datatype)
            
            if not type_format.regex.match(param):
                raise Exception('the format of input param "%s" (value: "%s") does not match type %s.'
                                % (param_name, param, input_slot.datatype))

            job_params[param_name] = type_format.class_object(param)

        return job_params


    def validate_outputs(self, raw_outputs, job_name):
        missing_fields = []
        node = self._lookup_node(job_name)
        for key, _ in node.output_slots.items():
            if raw_outputs.get(key) is None:
                missing_fields.append(key)
        
        if len(missing_fields):
            raise Exception('the generated outputs for job node "%s" are missing the fields: %s' 
                            % (job_name, missing_fields))


    def analyze(self, raw_outputs, job_name):
        result = []
        analyzer_func = self._lookup_analyzer(job_name)
        if analyzer_func:
            for condition in analyzer_func(raw_outputs):
                result.append({'type': condition.type, 'message': condition.message})
        return result


    def run(self, job_name, preview_mode=False, **kwargs):
        # execute query -- must return record generator
        # call job node's generate_outputs() method and pass in generator
        # if there is an analyzer attached to this job, pass it the outputs
        # return the raw and analyzed outputs
        node = self._lookup_node(job_name)
        query = node.generate_query(**kwargs)
        result = {}
        if preview_mode:
            print(query)
        else:
            job_config_group = self.config['jobs']
            exec_func_name = job_config_group[job_name]['executor_function']
            output_builder_func_name = job_config_group[job_name]['builder_function']
            # this is not a class being loaded, but a pythonic generator function
            query_executor = common.load_class(exec_func_name,  self.config['globals']['qa_logic_module'])

            # load the function which builds outputs from the query results
            output_builder_func = common.load_class(output_builder_func_name, self.config['globals']['qa_logic_module'])
            raw_outputs =  output_builder_func(query_executor(query, self.service_registry, **kwargs))
            self.validate_outputs(raw_outputs, job_name)
            result['job_output'] = raw_outputs

            result['analysis_ouptut'] = self.analyze(raw_outputs, job_name)

        return result


def read_params_from_args(arg_dict):
    params = {}
    if not arg_dict.get('--params'):
        return params

    param_string = args['--params'][0]
    param_tokens = [pstring.lstrip().rstrip() for pstring in param_string.split(PARAM_TOKEN_DELIMITER)]
    for token in param_tokens:
        if PARAM_DELIMITER not in token:
            raise Exception('the optional --params string must be in the form of name1:value1,...nameN:valueN')
        name = token.split(PARAM_DELIMITER)[0].lstrip().rstrip()
        value = token.split(PARAM_DELIMITER)[1].lstrip().rstrip()
        params[name] = value
    return params


def parse_param_string(input_param_string):
    '''parse an input string of the format 

    <name1>:<value1>,<name2>:<value2>,...

    and return a dictionary of name-value pairs
    '''

    params = {}
    tokens = input_param_string.split(',')
    for tok in tokens:
        param_tokens = tok.split(':')
        if len(param_tokens) != 2:
            raise Exception('Each parameter string in a job file must be of the format <name>:<value>')

        name = param_tokens[0]
        value = param_tokens[1]
        params[name] = value

    return params


def main(args):    
    configfile = args['<configfile>']
    yaml_config = common.read_config_file(configfile)
    preview_mode = False

    if args['--preview'] == True:
        preview_mode = True
        print('### Running in preview mode.', file=sys.stderr)

    if args['--list']:
        jobs = []
        if args['--verbose']:
            for job in yaml_config['jobs']:
                input_params = yaml_config['jobs'][job]['inputs'] or []
                output_params = yaml_config['jobs'][job]['outputs'] or []
                jobs.append({
                    'name': job,
                    'inputs': input_params,
                    'outputs': output_params
                })
        else:
            for job in yaml_config['jobs']:
                jobs.append(job)

        print(json.dumps(jobs))
        return

    job_runner = QaJobRunner(yaml_config)

    if args['--job']:
        jobname = args['<jobname>']
        #print(job_runner.get_sql_template(jobname))        
        input_params = read_params_from_args(args)

        job_params = job_runner.convert_input_params(jobname, **input_params)
        if preview_mode:
            job_runner._lookup_node(jobname).validate_input_types(**input_params)
            print(job_runner.get_sql_template(jobname).format(**input_params))
        else:
            print(json.dumps(job_runner.run(jobname, **job_params)))
        return

    elif args['--jobs']:
        job_strings = []
        if args['--file']:
            jobfile_name = args['<filename>']
            with open(jobfile_name) as f:
                for line in f:
                    job_strings.append(line.lstrip().rstrip())
        else:
            for line in sys.stdin:
                if sys.hexversion < 0x03000000:
                    line = line.decode('utf-8')
                job_strings.append(line.lstrip().rstrip())

            for job_string in job_strings:
                # Each job string is in the format <job_name> <param1>:<value1>,...
                job_tokens = job_string.split(' ')
                if len(job_tokens) != 2:
                    print('Each job string must be in the format <job_name> <param1>:<value1>,...')
                    return

                jobname = job_tokens[0]
                param_string = job_tokens[1]
                raw_params = parse_param_string(param_string)
                input_params = job_runner.convert_input_params(jobname, **raw_params)

                job_runner._lookup_node(jobname).validate_input_types(**input_params)
                if preview_mode:
                    print(job_runner.get_sql_template(jobname).format(**input_params))
                else:
                    print(json.dumps(job_runner.run(jobname, **input_params)))
        return


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

