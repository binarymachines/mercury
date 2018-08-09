#!/usr/bin/env python

import time
from journaling import *


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
