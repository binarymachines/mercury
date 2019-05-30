#!/usr/bin/env python


'''
DROP TRIGGER IF EXISTS {trigger_name} ON {schema}.{table_name};
CREATE TRIGGER {trigger_name} AFTER {db_op} ON {schema}.{table_name} 
FOR EACH ROW 
EXECUTE PROCEDURE {schema}.{db_proc_name}();
'''
