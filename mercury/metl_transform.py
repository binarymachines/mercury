#!/usr/bin/env python

'''Usage: transform_order_lineitems.py <initfile> [--segment=<segment_number>] [--frames <num_frames>] [--size <framesize>]
'''

from __future__ import division
import snap
from snap import common
import couchbasedbx as cbx
import sqldbx
import getpass
import datetime, time
import os
import json
import docopt
import logging
import yaml
from couchbase.n1ql import N1QLQuery
import constants as const
import cb_queries
import metl_utils as metl
from metl import jsonpretty, generate_order_key, generate_transformed_order_key, ReadingFrame


LOG_TAG = 'METL_transform'

platform_name_map = {
    'pos': 'other:point_of_sale',
    'storefront:web': 'custom_shop:web',
    'marketplace:web': 'marketplace:web',
    'marketplace:ios': 'marketplace:ios',
    'marketplace:android': 'marketplace:android'
}



class LineItem(object):
    def __init__(self, dict):
        for key, value in dict.iteritems():
            setattr(self, key, value)


    def calculate(self):
        pass



class MerchandiseLineItem(LineItem):
    def calculate(self):
        return self.sku_price * self.sku_quantity

    
class MerchandiseTaxLineItem(LineItem):
    def calculate(self):
        return self.merchandise_tax_amount


class ShippingLineItem(LineItem):
    def calculate(self):
        return self.shipping_amount


class ShippingTaxLineItem(LineItem):
    def calculate(self):
        return self.shipping_tax_amount


class DiscountLineItem(LineItem):
    def calculate(self):
        return -self.discount_amount


class InvoiceFeeLineItem(LineItem):
    def calculate(self):
        return self.invoice_fee_amount


class InvoiceFeeTaxLineItem(LineItem):
    def calculate(self):
        return self.invoice_fee_tax_amount



class OrderCalculator(object):
    def __init__(self):
        self.dispatch_tbl = {}
        self.dispatch_tbl['merchandise_lineitems'] = MerchandiseLineItem
        self.dispatch_tbl['merchandise_tax_lineitems'] = MerchandiseTaxLineItem
        self.dispatch_tbl['shipping_lineitems'] = ShippingLineItem
        self.dispatch_tbl['shipping_tax_lineitems'] = ShippingTaxLineItem
        self.dispatch_tbl['discount_lineitems'] = DiscountLineItem
        self.dispatch_tbl['invoice_fee_lineitems'] = InvoiceFeeLineItem
        self.dispatch_tbl['invoice_fee_tax_lineitems'] = InvoiceFeeTaxLineItem

        self.tax_field_names = [k for k in self.dispatch_tbl.keys() if 'tax' in k]
        

    def calculate_taxes(self, source_record):
        sum = 0
        for key in self.tax_field_names:
            lineitem_calculator = self.dispatch_tbl[key]
            lineitem_array = source_record[key]
            for item in lineitem_array:
                sum = sum + lineitem_calculator(item).calculate()

        return sum
            
                
    def calculate_total(self, source_record):
        sum = 0
        for key in self.dispatch_tbl.keys():
            lineitem_calculator = self.dispatch_tbl.get(key)
            if lineitem_calculator:
                collection = source_record[key]                
                subtotal = 0
                for item in  collection:
                    backout = item.get('is_included_in_related_line_item')
                    if not backout:
                        subtotal = subtotal + lineitem_calculator(item).calculate()        
                    elif backout and backout == 'false':
                        subtotal = subtotal + lineitem_calculator(item).calculate()        
                sum = sum + subtotal
                
        return sum
                
        
    

def get_total_merchandise_price(source_record):
    result = 0
    for item in source_record['merchandise_lineitems']:
        result = result + item['sku_price'] 

    return result



def recalc_merchandise_lineitem_prices(source_record):
    results = []
    for old_item in source_record['merchandise_lineitems']:
        item_price = old_item['sku_price']
        total_tax = source_record['merchandise_tax_lineitems'][0]['merchandise_tax_amount']
        
        new_item = {}
        new_item.update(old_item)

        merchandise_total = get_total_merchandise_price(source_record)
        merchandise_fraction = item_price / merchandise_total
        item_tax = merchandise_fraction * total_tax
        updated_price = old_item['sku_price'] - item_tax        
        new_item['sku_price'] = updated_price
        
        results.append(new_item)
        
    return results



class OrderTotalDiscrepancyError(Exception):
    def __init__(self, order_id):
        Exception.__init__(self, 'The sum of line item amounts does not match the total in order ID %s.' % (order_id))
        




def lineitem_sum_matches_order_total(source_record):    
    order_calc = OrderCalculator()    
    if order_calc.calculate(source_record) == source_record['total_purchase_amount']:
        return True
    return False

        

def transform(input_record):
    log = logging.getLogger('METL_transform')
    output_source_record = {}
    input_source_record = input_record.source_record    
    output_source_record.update(input_source_record) # make a deep copy of the inbound data

    # first take care of shopper platform names: change old nomenclature to new
    legacy_platform_name = input_source_record.get('shopper_platform')
    if not legacy_platform_name:
        # TODO: mark this record
        new_platform_name = 'NO_PLATFORM'
    else:
        new_platform_name = platform_name_map.get(legacy_platform_name)
        if not new_platform_name:
            raise Exception('No mapping for shopper platform name "%s".' % legacy_platform_name)
            
    output_source_record['shopper_platform'] = new_platform_name

    ocalc = OrderCalculator()
    calculated_order_total = ocalc.calculate_total(input_source_record)
    total_tax = ocalc.calculate_taxes(input_source_record)
    stated_order_total = input_source_record['total_purchase_amount']
    
    if not calculated_order_total ==  stated_order_total:
        #log.debug('mismatch between calculated & stated totals for order ID %s, recalculating...' % input_source_record['order_id'])
        # maybe the tax needs to be backed out
        difference = abs(calculated_order_total - stated_order_total)
        
        if int(difference) == int(total_tax):

            #log.info('>>> backing out the tax...')
            updated_items = recalc_merchandise_lineitem_prices(input_source_record)
            output_source_record['merchandise_lineitems'] = updated_items

            updated_total = ocalc.calculate_total(output_source_record)            

            if not updated_total == stated_order_total:
                log.debug('>>> unresolvable numerical error in order ID %d.' % input_source_record['order_id'])
            
        else:
            log.debug('### possible numerical error in order ID %d.' % input_source_record['order_id'])

    target_rec = { 'status': 'processed',
                           'segment': input_record.segment,
                           'timestamp': datetime.datetime.now().isoformat(),
                           'source_record': output_source_record}
    
    return target_rec
    

    

def get_raw_records(segment_num, reading_frame, record_id_queue, couchbase_pm):
    query_limit = reading_frame.size
    query_offset = reading_frame.size * reading_frame.index_number
    
    query_string = cb_queries.raw_record_segment_query.format(couchbase_pm.bucket_name, query_limit, query_offset)
    
    qry = N1QLQuery(query_string)
    results = []
    for record in couchbase_pm.bucket.n1ql_query(qry):        
        results.append(cbx.CouchbaseRecordBuilder(const.RECTYPE_ORDER).add_fields(record[couchbase_pm.bucket_name]).build())
    return results


def get_raw_records_optimized(segment_num, reading_frame, record_id_queue, couchbase_pm):
    range_start = reading_frame.index_number * reading_frame.size
    range_end = range_start + reading_frame.size - 1    

    ids = record_id_queue.range(range_start, range_end)
    results = []
    for id in ids:
        raw_record = couchbase_pm.lookup_record_raw(id)        
        results.append(cbx.CouchbaseRecordBuilder(const.RECTYPE_ORDER).add_fields(raw_record).build())

    return results


def main(args):
    window_mode = False
    num_reading_frames = int(args.get('<num_frames>', -1))
    reading_frame_size = int(args.get('<framesize>', -1))
    segment = -1
    if args.get('--segment') is not None:
        segment = int(args['--segment'])
    
    initial_reading_frame = None    
    init_filename = args['<initfile>']
    
    yaml_config = common.read_config_file(init_filename)
    pipeline_id = yaml_config['globals']['pipeline_id']

    log_directory = yaml_config['globals']['log_directory']
    job_id = metl.generate_job_id('transform', pipeline_id)
    log_filename = metl.generate_logfile_name(job_id)
    log = metl.init_logging(LOG_TAG, os.path.join(log_directory, log_filename), logging.INFO)

    print '%s script started at %s, logging to %s...' % (LOG_TAG, datetime.datetime.now().isoformat(), log_filename)
    
    service_objects = snap.initialize_services(yaml_config, log)
    so_registry = common.ServiceObjectRegistry(service_objects)
    redis_svc = so_registry.lookup('redis')
    raw_record_id_queue = redis_svc.get_raw_record_queue(pipeline_id)
    transformed_queue = redis_svc.get_transformed_record_queue(pipeline_id)
    
    couchbase = so_registry.lookup('couchbase')
    couchbase.data_manager.register_keygen_function(const.RECTYPE_ORDER, generate_order_key)
    couchbase.data_manager.register_keygen_function(const.RECTYPE_ORDER_TRANSFORMED, generate_transformed_order_key)

    start_time = time.time()
    log.info('starting transform at %s.' % str(datetime.datetime.now()))
    paging_context = metl.PagingContext(num_reading_frames, reading_frame_size)
    reading_frame = paging_context.initial_reading_frame()
    pages_remaining = paging_context.num_frames_to_read
    num_records_processed = 0
    
    while pages_remaining:
        results = get_raw_records_optimized(segment, reading_frame, raw_record_id_queue, couchbase.data_manager)
        for raw_rec in results:
            try:
                cooked_rec = transform(raw_rec)
                key = couchbase.insert_record(const.RECTYPE_ORDER_TRANSFORMED, cooked_rec)
                transformed_queue.push(key)
            except Exception, err:                
                log.error('%s thrown while transforming record with order ID %s: %s' % (err.__class__.__name__, raw_rec.source_record['order_id'], err.message))
                
            num_records_processed += 1
            
            if num_records_processed % 100000 == 0:
                log.info('+++ %d order records transformed.' % (num_records_processed))
            
        reading_frame = reading_frame.shift_right()
        pages_remaining -= 1
        
    end_time = time.time()
    log.info('%d order records transformed.' % (num_records_processed))
    log.info('Total running time %s' % (metl.hms_string(end_time - start_time)))
        
        

if __name__=='__main__':
    args = docopt.docopt(__doc__)
    main(args)
