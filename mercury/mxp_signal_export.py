#!/usr/bin/env python

'''Usage: mxp_signal_export.py <initfile> (--from <start_date>) (--to <end_date>) (--file <output_file>)
'''


import docopt
import json
import sys, os
import logging
import requests
import base64
import datetime, time
import arrow
from mxpanel import Signal



def pull_mixpanel_signals(start_date, end_date, file_descriptor):
    MIXPANEL_ENDPOINT = 'https://data.mixpanel.com/api/2.0/export'
    MIXPANEL_API_KEY = '15d434cfb3b2e7192547aa51776e3e00'
    MIXPANEL_API_SECRET = '8e30075868304c041a85ef6b86ef6ba0'
    
    payload = {'from_date': start_date,
               'to_date': end_date,
               'format': 'json',
               'expire': int(time.time()) + 600}

    auth = base64.b64encode(MIXPANEL_API_SECRET).decode("ascii")
    request_headers = { 'Content-Type': 'application/json',
                        'Authorization': 'Basic %s' % auth }

    print 'issuing mixpanel request URL...'
    r = requests.get(MIXPANEL_ENDPOINT, headers=request_headers, params=payload)
    
    if r.status_code != 200:
        print '### Response body: %s' %r.text
        r.raise_for_status()
    
    print 'writing output...'
    for chunk in r.iter_content(chunk_size=4096):
        file_descriptor.write(chunk)


    
def main(args):
    init_file = args['<initfile>']
    start_date_string = args['<start_date>']
    end_date_string = args['<end_date>']
    output_filename = args['<output_file>']

    start_date = arrow.get(start_date_string).format('YYYY-MM-DD')
    end_date = arrow.get(end_date_string).format('YYYY-MM-DD')

    print 'Exporting mixpanel signals from %s to %s, target file is %s...' % (start_date, end_date, output_filename)

    with open(output_filename, 'wb') as f:
        pull_mixpanel_signals(start_date, end_date, f)

    print 'Done.'
        

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
