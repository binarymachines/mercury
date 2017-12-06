#!/usr/bin/env python

'''Usage: dgen_dates.py (--start <start_date>) (--end <end_date>)
'''




import arrow
from datetime import datetime
import tdxutils as tdx
import docopt
from snap import csvutils as csvu



q_length = 3
quarters = []
for q in range(1, 12, 3):
    months = range(q, q+3)
    quarters.append(months)


    
def day_of_week(target_date):
    return target_date.isoweekday()


def day_of_month(target_date):
    return target_date.day


def day_of_year(target_date):
    return int(target_date.format('DDDD'))


def month(target_date):
    return target_date.month


def quarter(target_date):
    target_month = month(target_date)

    quarter_number = 1
    for quarter in quarters:
        if target_month in quarter:
            return quarter_number
        quarter_number += 1
        


def year(target_date):
    return int(target_date.format('YYYY'))



def week_start_date(target_date):
    days_into_week = day_of_week(target_date)-1
    if days_into_week == 0:
        result = target_date
    result = target_date.replace(days=-days_into_week)

    return result.format('YYYY-MM-DD')
        
    

def dates_record(date, id):
    result = {}
    result['id'] = id
    result['date_value'] = date.format('YYYY-MM-DD')
    result['day_of_week'] = str(day_of_week(date))
    result['day_of_month'] = str(day_of_month(date))
    result['day_of_year'] = str(day_of_year(date))
    result['month'] = str(month(date))
    result['quarter'] = str(quarter(date))
    result['year'] = str(year(date))
    result['week_start_date'] = week_start_date(date)

    return result





def main(args):
    
    start_date_string = args['<start_date>']      
    end_date_string = args['<end_date>']

    start_date = arrow.get(start_date_string)
    end_date = arrow.get(end_date_string)

    dates = []
    counter = 1
    for d in arrow.Arrow.range('day', start_date, end_date):
        dates.append(dates_record(d, counter))
        counter += 1

    
    rmb = csvu.CSVRecordMapBuilder()
    rmb.add_field('id', int)
    rmb.add_field('date_value', datetime)
    rmb.add_field('day_of_week', int)
    rmb.add_field('day_of_month', int)
    rmb.add_field('day_of_year', int)
    rmb.add_field('month', int)
    rmb.add_field('quarter', int)
    rmb.add_field('year', int)
    rmb.add_field('week_start_date', datetime)    

    map = rmb.build()

    for d in dates:
        print(map.dictionary_to_row(d, delimiter='|'))

        
if __name__=='__main__':
    args = docopt.docopt(__doc__)
    main(args)
    
