#!/usr/bin/env python


from journaling import journal, CouchbaseOpLogWriter, OpLogEntry, TimestampField, StatusField


      
owriter = CouchbaseOpLogWriter(hostname='localhost', bucket='default')
oplog_entry_1 = OpLogEntry().add_field(TimestampField()).add_field(StatusField('start'))
oplog_entry_2 = OpLogEntry().add_field(TimestampField()).add_field(StatusField('end'))
mutable_oplog_entry = OpLogEntry().add_field(TimestampField()).add_field(StatusField('incomplete'))



@journal('timelog_test_single_ended', owriter, oplog_entry_1)
def test_single_ended_journaling():
    print '### This is a single-ended journaled operation.'

    

    
@journal('timelog_test_double_ended', owriter, oplog_entry_1, oplog_entry_2)
def test_double_ended_journaling():
    print('### This is a double-ended journaled operation.')



    
@journal('delta_journaling_test'), owriter, mutable_oplog_entry):
def test_delta_journaling():
    print('### This is a journaled operation with a mutable oplog entry.')
    
     
def test_context_manager_mode():
    with journal('context_mgr_test_op', oplog) as j:
        print('### This is also a journaled operation.')


       
def main():
    test_single_ended_journaling()
    test_double_ended_journaling()
    test_delta_journaling()
    #test_context_manager_mode()
    #test_load_record()
    
if __name__ == '__main__':
    main()
