#!/usr/bin/env python

import os
import sys
import hashlib
import logging
import mx_services as mxsvc
import mxtypes

from snap import sqldbx
from sqlalchemy_utils import UUIDType
from sqlalchemy_utils import types as sqltypes
from sqlalchemy import Integer, String, and_
from sqlalchemy.orm import joinedload
from sqlalchemy.schema import Sequence


def lookup_function(db_session, log):
    return None
        

def main():
    log = logging.getLogger('test')

    password = os.getenv('POSTGRESQL_PASSWORD')
    pso = mxsvc.PostgresServiceObject(log,
                                      host='',
                                      username='',
                                      password=password,
                                      database='mercury',
                                      schema='ingest')

    print '### %s' % pso.db.jdbcURL()
    db_session = pso.data_manager.getSession()



if __name__ == '__main__':
    main()



