#!/usr/bin/env python

'''Usage:    forge.py <initfile>

'''

from cmd import Cmd
import docopt
import os, sys
import yaml
import logging
import metl_utils as metl
import cli_tools as cli
import snap
from snap import common



mkfact_options = [{'value': 'add_fact_attr', 'label': 'Add fact attribute'},
                  {'value': 'add_dimension', 'label': 'Add dimension'}]

attribute_type_options = [{'value': 'string', 'label': 'String'},
                          {'value': 'int', 'label': 'Integer'}, 
                          {'value': 'float', 'label': 'Float'},
                          {'value': 'date', 'label': 'Date'},
                          {'value': 'timestamp', 'label': 'Timestamp'},
                          {'value': 'bool', 'label': 'Boolean'},
                          {'value': 'char', 'label': 'Character'}]

type_amendments = { 'string': 'length' }



class Field(object):
    def __init__(self, f_name, f_type):
        self._name = f_name
        self._type = f_type

    def get_name(self):
        return self._name

    def get_type(self):
        return self._type

    def data(self):
        return { 'name': self._name, 'type': self._type }


class Dimension(object):
    def __init__(self, name, fields=[]):
        self._name = name
        self._fields = fields

    @property 
    def name(self):
        return self._name


    def add_field(self, f_name, f_type):
        fields = copy.deepcopy(self._fields)
        fields.append(Field(f_name, f_type))
        return Dimension(self.name, fields)


    @property
    def fields(self):
        return iter(self._fields)


    def data(self):
        return {'name': self.name, 'fields': [f.data() for f in self.fields]}


class FactAttribute(object):
    def __init__(self, name, data_type, **kwargs):
        self._name = name
        self._type = data_type
        self.amendments = kwargs

    
    @property
    def name(self):
        return self._name


    @property
    def datatype(self):
        return self._type

    
    def data(self):
        return {'name': self.name, 'type': self._type}



class ForgeCLI(Cmd):
    def __init__(self, name, service_obj_registry):
        Cmd.__init__(self)
        self.name = name
        self.prompt = '[%s] ' % self.name
        self.service_registry = service_obj_registry
        self.facts = {}
        #self.replay_stack = Stack()


    def prompt_for_value(self, value_name):
        #cli.Notifier('fact', current_cmd).show()
        parameter_value = cli.InputPrompt('enter value for <%s>: ' % value_name).show()
        return parameter_value


    def do_quit(self, *cmd_args):
        print('%s CLI exiting.' % self.name)
        print(metl.jsonpretty(self.facts))
        raise SystemExit


    do_q = do_quit


    def create_dimension_for_fact(self, fact_name):
        fields = []
        print('Creating a new dimension for fact %s...' % fact_name)
        dim_name = cli.InputPrompt('Enter a name for this dimension').show()
        if dim_name:
            print('Add 1 or more fields to this dimension.')
            while True:
                field_name = cli.InputPrompt('field name').show()
                if not field_name:
                    break
                field_type = cli.MenuPrompt('field type', attribute_type_options).show()
                if not field_type:
                    break
                print('> Added new field "%s" to fact dimension %s->%s.' % (field_name, fact_name, dim_name))
                fields.append(Field(field_name, field_type))
                should_continue = cli.InputPrompt('Add another field (Y/n)?', 'y').show()
                if should_continue == 'n':
                    break
            return Dimension(dim_name, fields)
        return None


    def create_attribute_for_fact(self, fact_name):
        attr_name = cli.InputPrompt('Add new attribute to fact "%s"' % fact_name).show()
        if attr_name:
            attr_type = cli.MenuPrompt('attribute type', attribute_type_options).show()
            if attr_type:
                return FactAttribute(attr_name, attr_type)


    def update_fact(self, fact_name):
        opt_prompt = cli.MenuPrompt('Select fact operation', 
                                    mkfact_options)
        operation = opt_prompt.show()
        while True:            
            if operation == 'add_dimension':
                newdim = self.create_dimension_for_fact(fact_name)
                if newdim:
                    self.facts[fact_name]['dimensions'].append(newdim.data())
                    should_continue = cli.InputPrompt('Add another dimension (Y/n)?', 'y').show().lower()
                    if should_continue == 'n': 
                        break
                    
            if operation == 'add_fact_attr':
                newattr = self.create_attribute_for_fact(fact_name)
                if newattr:
                    self.facts[fact_name]['fields'].append(newattr.data())
                    should_continue = cli.InputPrompt('Add another attribute (Y/n)?', 'y').show().lower()
                    if should_continue == 'n': 
                        break


    def do_mkfact(self, *cmd_args):
        if not len(*cmd_args):
            print('mkfact command requires the fact name.')
            return
        fact_name = cmd_args[0]
        self.facts[fact_name] = {'dimensions': [], 'fields': []}
            
        print('Creating new fact: %s' % fact_name)
        self.update_fact(fact_name)
            

    def do_lsfact(self, *cmd_args):
        print('> Current facts:')
        print(' \n'.join(self.facts.keys()))


    def do_chfact(self, *cmd_args):
        if not len(*cmd_args):
            print('chfact command requires the fact name.')
            return

        fact_name = cmd_args[0]
        if not self.facts.get(fact_name):
            print('no fact registered with name "".' % fact_name)
            return

        self.update_fact(fact_name)
        
    


def main(args):
    init_filename = args['<initfile>']
    yaml_config = common.read_config_file(init_filename)
    log_directory = yaml_config['globals']['log_directory']
    log_filename = 'forge.log'
    log = metl.init_logging('mx_forge', os.path.join(log_directory, log_filename), logging.DEBUG)
    
    services = snap.initialize_services(yaml_config, log)
    so_registry = common.ServiceObjectRegistry(services)
    forge_cli = ForgeCLI('forge', so_registry)
    forge_cli.cmdloop()
    

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)

