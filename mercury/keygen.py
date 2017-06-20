#!/usr/bin/env python

import constants as const



def generate_date_cache_key(date_value):
    return 'date_%s' % date_value.isoformat()


def generate_brand_cache_key(brand_id):
    return 'brand_%d' % brand_id


def generate_user_cache_key(user_id):
    return 'user_%d' % user_id


def generate_platform_cache_key(platform_id):
    return 'platform_%s' % platform_id


def generate_order_lineitem_record_key(record):
    return '%s_orderid_%s_%s_%s' (const.RECTYPE_ORDER_LINEITEM, order_id, record_type_name, lineitem_id)

    
def generate_touch_key(record):
    return '%s_touch_%s.%s' % (const.RECTYPE_TOUCH, record.generator_id, str(record.timestamp))


def generate_transformed_touch_key(record):
    return '%s_touch_%s.%s' % (const.RECTYPE_TOUCH_TRANSFORMED, record.generator_id, str(record.timestamp))
