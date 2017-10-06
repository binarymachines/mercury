#!/usr/bin/env python

'''Usage:  mconsole [pipeline_spec_file]

'''

import docopt
from docopt import docopt as docopt_func
from docopt import DocoptExit
from snap import common
from cmd import Cmd
import os, sys


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

            print '\nPlease specify one or more valid command parameters.'
            print e
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



class PipelineMeta(object):
    def __init__(self, name):
        self._name = name


    @property
    def name(self):
        return self._name



class StageMeta(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('name', 'stage_type'):
        kwreader.read(kwargs)
        self._name = kwreader.get_value('name')
        self._type = kwreader.get_value('stage_type')

    @property
    def name(self):
        return self._name

    @property
    def stage_type(self):
        return self._type


    
class IngestSchemaField(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('name', 'datatype', 'required')
        kwreader.read(kwargs)
        self._name = kwreader.get_value('name')
        self._type = kwreader.get_value('datatype')
        self._required = kwreader.get_value('required')


    @property
    def name(self):
        return self._name


    @property
    def datatype(self):
        return self._type


    @property
    def required(self):
        return self._required
        
        

    
class IngestSchema(object):
    def __init__(self, **kwargs):
        self._fields = []

        
    def add_field(self, name, field_type, is_required):
        self._fields.append(IngestSchemaField(name=name,
                                              datatype=field_type,
                                              required=is_required))
        

class TransformSchemaField(object):
    def __init__(self, **kwargs):
        

        
class TransformSchema(object):
    def __init__(self, **kwargs):
        self._fields = []


    def add_field(self, **kwargs):
        kwreader = common.KeywordArgReader('name', 'source')
        kwreader.read(kwargs)
        
        field_name = kwreader.get_value('name')
        source = kwreader.get_value('source')

        if source == 'record':
            if not kwargs.get('key'):
                raise Exception('A transform schema field with source type "record" must specify a key.')

        if source == 'lookup':
            pass

        if source == 'value':
            if not kwargs.get('value'):
                raise Exception('A transform schema field with source type "value" must specify a value.')

        self.fields.append(TransformSchemaField(**kwargs))


        
class MConsoleCLI(Cmd):
    def __init__(self, pipeline_name, **kwargs):
        Cmd.__init__(self)

        kwreader = common.KeywordArgReader(*[])
        kwreader.read(**kwargs)
        self.set_name(pipeline_name or 'anonymous')


    def set_name(self, pipeline_name):
        self.name = pipeline_name
        self.prompt = '[pipeline:%s]> ' % self.name


    @docopt_cmd
    def do_mkpl(self, arg):
        '''Usage:
            mkpl <pipeline_name>
        '''
        pass

    def do_EOF(self, arg):
        print '\n'
        return True


    @docopt_cmd
    def do_mkstage(self, arg):
        '''Usage:    mkstage (extract | transform | load)
        '''
        print arg
        #print 'make pipeline stage for %s' % 
        pass
    


def main(args):

    MConsoleCLI(None).cmdloop()


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
