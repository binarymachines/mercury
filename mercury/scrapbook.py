


class OrderBatchReader(object):
    def __init__(self):        
        self.query_template = cb_queries.raw_order_record_segment_query

        
    def get_recordset(self, reading_frame, cb_persistence_mgr):
        query = self.query_template.format(reading_frame.size, reading_frame.index_number * reading_frame.size)
        print 'executing query:\n%s' % query
        
        records = []
        
        for record in cb_persistence_mgr.bucket.n1ql_query(query):           
            records.append(record['tdx_data'])
                
        return records




def extract(extraction_query, redshift_database, couchbase_pm):
    keys = []
    with redshift_database.engine.connect() as conn:
        result_set = conn.execute(extraction_query)                
        for record in result_set:
            rdict = {}
            for key in record.iterkeys():
                if key == 'order_placement_timestamp':
                    rdict[key] = record[key].isoformat()
                else:
                    rdict[key] = record[key]
            couchbase_key = write_raw_record_to_staging(rdict, couchbase_pm)
            keys.append(couchbase_key)

    return keys
    


def write_raw_record_to_staging(record_dict, cb_persistence_mgr):
    
    builder = cbx.CouchbaseRecordBuilder(const.RECTYPE_ORDER_LINEITEM)
    builder.add_field('source_record', record_dict)
    builder.add_field('timestamp', datetime.datetime.now().isoformat())
    builder.add_field('segment', 1)
    builder.add_field('status', 'unprocessed')
    rec = builder.build()

    return cb_persistence_mgr.insert_record(rec)

