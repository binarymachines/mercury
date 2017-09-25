#!/usr/bin/env python

'''Usage:  mconsole [pipeline_spec_file]

'''

import docopt
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
    


def main(args):

    MConsoleCLI(None).cmdloop()


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
