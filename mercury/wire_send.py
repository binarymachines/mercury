#!/usr/bin/env python

'''Usage: tdx_console.py
'''


import docopt
import json
import pika
import sys, os
import logging
import requests
import base64
import datetime, time
from mxpanel import Signal


#MSG_EXCHANGE = 'mixpanel_signals'
MSG_EXCHANGE = 'tdx.mixpanel.signals'



class SignalToRouteMapper(object):

    def __init__(self):
        self.sr_map = {}
        self.sr_map['Dashboard'] = 'mxp.signals.dashboard'
        self.sr_map['Marketplace'] = 'mxp.signals.marketplace'
        self.sr_map['Mobile'] = 'mxp.signals.mobile'
        self.sr_map['Colophon'] = 'mxp.signals.customshop'
        self.sr_map['Checkout'] = 'mxp.signals.checkout'
        self.sr_map['Notification'] = 'mxp.signals.notification'
        self.sr_map['Order Tracking'] = 'mxp.signals.tracking'

        self.default_routing_key = 'mxp.signals.default'

        
    def map(self, signal):
        return self.sr_map.get(signal.channel) or self.default_routing_key
        


def mk_dummy_signal():
    #name = '$app_open'
    name = 'Order Tracking:foo'
    timestamp = '1420163601'
    userid = '149c98001ac4a3-08644e6fd-78354a49-fa000-149c98001ad5c8'

    properties = '''
    {"time":1420163601,
    "distinct_id":"149c98001ac4a3-08644e6fd-78354a49-fa000-149c98001ad5c8",
    "$app_release":"1.4",
    "$app_version":"2873",
    "$carrier":"AT&T",
    "$city":"Plaquemine",
    "$ios_ifa":"9FC98D79-2BA2-4867-8FAF-080A0752BF87",
    "$lib_version":"2.5.0",
    "$manufacturer":"Apple",
    "$model":"iPhone7,2",
    "$os":"iPhone OS",
    "$os_version":"8.1.2",
    "$radio":"None",
    "$region":"Louisiana",
    "$screen_height":568,
    "$screen_width":320,
    "attribution_yozio_device_id":"efe92620c49fb67c57410577bdd6561aff95d1cd",
    "campaign":"Organic",
    "media_source":"Organic",
    "mp_country_code":"US",
    "mp_device_model":"iPhone7,2",
    "mp_lib":"iphone"}
    '''
    
    return Signal(name, timestamp, userid, properties)



def pull_mixpanel_signals():
    MIXPANEL_ENDPOINT = 'https://data.mixpanel.com/api/2.0/export'
    MIXPANEL_API_KEY = '15d434cfb3b2e7192547aa51776e3e00'
    MIXPANEL_API_SECRET = '8e30075868304c041a85ef6b86ef6ba0'
    
    from_date = '2016-11-1'
    to_date = '2016-11-2'

    payload = {'from_date': from_date,
               'to_date': to_date,
               'format': 'json',
               'expire': int(time.time()) + 600}

    auth = base64.b64encode(MIXPANEL_API_SECRET).decode("ascii")
    request_headers = { 'Content-Type': 'application/json',
                        'Authorization': 'Basic %s' % auth }
                
    r = requests.get(MIXPANEL_ENDPOINT, headers=request_headers, params=payload)

    if r.status_code != 200:
        print '### Response body: %s' %r.text
        r.raise_for_status()
    
    with open('mixpanel_dump.json', 'wb') as fd:
        for chunk in r.iter_content(chunk_size=4096):
            fd.write(chunk)


    



def main(args):

    # Parse CLODUAMQP_URL (fallback to localhost)
    url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
    params = pika.URLParameters(url)
    params.socket_timeout = 5

    connection = pika.BlockingConnection(params)
    channel = connection.channel()


    channel.exchange_declare(exchange=MSG_EXCHANGE,
                             type='topic',
                             durable=True)


    #signal = mk_dummy_signal()


    mapper = SignalToRouteMapper()    
    
    
    with open('mixpanel_dump.json') as f:
        id = 1
        for line in iter(f):
            signal_data = json.loads(line)
            signal = Signal(signal_data['event'], datetime.datetime.now().isoformat(), id, signal_data['properties'])
            routekey = mapper.map(signal)
            print '### queuing signal ID %d, routing key is %s' % (id, routekey)
            channel.basic_publish(exchange=MSG_EXCHANGE, routing_key=routekey, body=json.dumps(signal.__dict__))
            id += 1
            
    


    #print '###  converted signal as JSON string:'
    #print json.dumps(signal.__dict__, indent=4)
    
    print 'Published message(s) to queue, exiting...'

    connection.close()


    #pull_mixpanel_signals()
    
        
    

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
