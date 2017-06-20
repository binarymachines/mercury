#!/usr/bin/env python


# journaling types
RECTYPE_PIPELINE = 'metl_pipeline'
RECTYPE_JOB = 'metl_job'
RECTYPE_OPERATION = 'metl_op'

# bucket names

DATA_BUCKET = 'metl_data'
JOURNAL_BUCKET = 'metl_journal'
CACHE_BUCKET = 'metl_cache'


STATUS_UNPROCESSED = 'unprocessed'
STATUS_PROCESSED = 'processed'
BORDER = '\n----------------------------------------\n'
