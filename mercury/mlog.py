#!/usr/bin/env python


import os, sys
import json
from snap import common
import datetime
import traceback
from collections import namedtuple



def mlog(log_message, file_handle=sys.stderr, **kwargs):

    log_entry = {        
        'msg': log_message,
        'emitted_at': datetime.datetime.now().isoformat()
    }

    log_entry.update(kwargs)
    print(json.dumps(log_entry), file=file_handle)


def mlog_err(exception, file_handle=sys.stderr, **kwargs):

    error_details = {
        'error_message': str(exception),
        'exception_type': exception.__class__.__name__,
        'traceback': traceback.format_list(traceback.extract_tb(exception.__traceback__))        
    }

    error_details.update(kwargs)

    mlog(f'!!! Exception thrown.', file_handle, **error_details)

    