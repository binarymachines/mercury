#!/usr/bin/env python

import context
import unittest
from mercury import mx_utils as mx
from mercury import jobs as j
from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner



class JobCreationAndManagementTest(unittest.TestCase):

    def setUp(self):
        #self.required_job_attrs = []
        pass


    def tearDown(self):
        pass

    
    def test_default_job_creation_gives_job_with_required_attrs(self):
        j1 = j.Job(name='new_job', owner_id='sduncan', description='some kind of task')
        self.assertEquals(j1.name, 'new_job')
        self.assertEquals(j1.owner_id, 'sduncan')
        self.assertEquals(j1.description, 'some kind of task')
        
        
    def test_bad_job_constructor_call_throws_correct_exception(self):
        with self.assertRaises(j.JobCreationException) as context:
            j1 = j.Job(name='new_job')
            
        
    def test_add_job_returns_job_id(self):
        jm = j.JobManager()
        id = jm.add_job(name='nj', owner_id='sd', description='some kind of task')
        self.assertIsNotNone(id)
        

    def test_job_manager_returns_correct_job_count(self):
        jm = j.JobManager()
        jm.add_job(name='nj', owner_id='sd', description='some kind of task')
        jm.add_job(name='second job', owner_id='sd', description='some kind of second task')
        self.assertEquals(jm.get_job_count(), 2)

        
    def test_get_job_status_by_id_returns_correct_job(self):
        test_status = 'adding to postgres'
        jm = j.JobManager()
        id = jm.add_job(name='nj', owner_id='sd', description='some kind of task', status=test_status)
        self.assertEquals(jm.get_job_status_by_id(id), test_status)


    def test_get_jobs_by_owner_id_returns_correct_job_count(self):
        id1 = 'id1'
        id2 = 'id2'
        jm = j.JobManager()
        jm.add_job(name='nj', owner_id=id1, description='some kind of task')
        jm.add_job(name='nj', owner_id=id1, description='some kind of task')
        jm.add_job(name='nj', owner_id=id2, description='some kind of task')
        self.assertEquals(len(jm.get_jobs_by_owner_id(id1)), 2)

        
    def test_get_jobs_by_owner_id_returns_correct_jobs(self):
        user1 = 'userid1'
        user2 = 'userid2'
        jm = j.JobManager()
        jid1 = jm.add_job(name='job1', owner_id=user1, description='some kind of task')
        jid2 = jm.add_job(name='job2', owner_id=user1, description='some kind of task')
        jid3 = jm.add_job(name='job3', owner_id=user2, description='some kind of task')

        job_ids_owned_by_user1 = set([jid1, jid2])
        jobs = jm.get_jobs_by_owner_id(user1)
        for job in jobs:
            self.assertIn(job.id, job_ids_owned_by_user1)


    def test_job_manager_can_update_job_status_by_id(self):
        user1 = 'userid1'
        user2 = 'userid2'
        default_status = 'init'
        new_status = 'running'
        jm = j.JobManager()
        jid1 = jm.add_job(name='job1', owner_id=user1, description='some kind of task', status=default_status)
        jid2 = jm.add_job(name='job2', owner_id=user1, description='some kind of task', status=default_status)
        jid3 = jm.add_job(name='job3', owner_id=user2, description='some kind of task', status=default_status)

        jm.update_job_status_by_id(jid1, new_status)
        updated_job = jm.lookup_job(jid1)
        self.assertEqual(updated_job.status, new_status)


    def test_get_all_jobs_returns_correct_jobs(self):
        user1 = 'userid1'
        user2 = 'userid2'
        jm = j.JobManager()
        jid1 = jm.add_job(name='job1', owner_id=user1, description='some kind of task')
        jid2 = jm.add_job(name='job2', owner_id=user1, description='some kind of task')
        jid3 = jm.add_job(name='job3', owner_id=user2, description='some kind of task')
        
        job_ids = set([jid1, jid2, jid3])
        jobs = jm.get_all_jobs()
        for job in jobs:
            self.assertIn(job.id, job_ids)


if __name__ == '__main__':
    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
        
    unittest.main(testRunner=runner)