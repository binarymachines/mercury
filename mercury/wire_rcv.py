#!/usr/bin/env python

'''Usage: tdx_sigrcv.py <binding_keys>
'''



import docopt
import pika
import sys, os


MSG_EXCHANGE = 'tdx.mixpanel.signals'


def main(args):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=MSG_EXCHANGE,
                             type='topic',
                             durable=True)


    method_frame, header_frame, body = channel.basic_get('tdx_mxpsig_dashboard')

    if method_frame:
        print method_frame, header_frame, body
        channel.basic_ack(method_frame.delivery_tag)
    else:
        print 'No message returned'

        
    
    '''
    result = channel.queue_declare(exclusive=False)
    
    queue_name = result.method.queue
    binding_keys = args['<binding_keys>'].split(',')
    for binding_key in binding_keys:

        print 'using binding key: %s' % binding_key
        channel.queue_bind(exchange=MSG_EXCHANGE,
                           queue=queue_name,
                           routing_key=binding_key)

        
    print(' [*] Waiting for messages routed with keys %s. To exit press CTRL+C' % binding_keys)



    def callback(ch, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))

    channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)


    channel.start_consuming()
    '''


if __name__=='__main__':
    args = docopt.docopt(__doc__)
    main(args)
