#!/usr/bin/env python

import time
from journaling import *


global_count_log = CountLog()
global_time_log = TimeLog()


@counter('call annotated function', global_count_log)
def some_func():
    pass


@stopwatch('call timed function', global_time_log)
def timed_function():
    time.sleep(2)
    


def main():
    print('hello journal')
    
    tlog = TimeLog()

    with stopwatch('dummy operation', tlog):
        time.sleep(3)

    with stopwatch('other operation', tlog):
        time.sleep(1)
        
    for key, value in tlog.data.items():
        print('%s: %s' % (key, value))

    print('goodbye journal')

if __name__ == '__main__':
    main()
