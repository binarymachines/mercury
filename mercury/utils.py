#!/usr/bin/env python

from os import system, name
import itertools
from enum import Enum

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