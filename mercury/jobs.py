#!/usr/bin/env python

from snap import common
import datetime


REQUIRED_JOB_FIELDS = ['name', 'owner_id', 'description']


class NoSuchJobException(Exception):
    def __init__(self, job_id):
        Exception.__init__(self, 'No Job with ID %s found.' % job_id)


class JobCreationException(Exception):
    def __init__(self, error_msg):
        Exception.__init__(self, 'Error(s) attempting to create new Job: %s' % error_msg)



class Job(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader(*REQUIRED_JOB_FIELDS)
        kwreader.read(**kwargs)
        self._name = kwreader.get_value('name')
        self._description = kwreader.get_value('description')
        self._owner_id = kwreader.get_value('owner_id')
        self._id = kwargs.get('id') # this is an optional kwarg
        self._status = kwargs.get('status') # same here


    @property
    def id(self):
        return self._id


    @property
    def name(self):
        return self._name


    @property
    def status(self):
        return self._status


    @property
    def description(self):
        return self._description


    @property
    def owner_id(self):
        return self._owner_id


    def update_status(self, new_status):
        new_job_params = {
            'id':self._id,
            'name':self._name,
            'owner_id':self._owner_id,
            'description':self._description,
            'status':new_status
        }

        return Job(**new_job_params)


    def data(self):
        return {
            'id':self._id,
            'name':self._name,
            'owner_id':self._owner_id,
            'description':self._description,
            'status':self._status
        }

    def __str__(self):
        return 'JOB: name: %s, owner: %s, desc: %s' % (self._name, self._owner_id, self._description)



class JobManager(object):
    def __init__(self, **kwargs):
        self._jobs = {}


    def _generate_job_id(self, **kwargs):
        new_id = '%s.%s' % (kwargs['owner_id'], datetime.datetime.now().isoformat())
        return new_id


    def add_job(self, **kwargs):
        kwarg_reader = common.KeywordArgReader(*REQUIRED_JOB_FIELDS).read(**kwargs)
        job_id = self._generate_job_id(**kwargs)

        job_params = kwargs
        job_params.update({'id': job_id})
        self._jobs[job_id] = Job(**job_params)
        return job_id


    def lookup_job(self, job_id):
        j = self._jobs.get(job_id)
        if not j:
            raise NoSuchJobException(job_id)
        return j


    def get_job_count(self):
        return len(self._jobs.keys())


    def update_job_status_by_id(self, job_id, status):
        target_job = self._jobs.get(job_id)
        if not target_job:
            raise NoSuchJobException(job_id)
        updated_job = target_job.update_status(status)
        self._jobs[job_id] = updated_job
        return updated_job


    def get_job_status_by_id(self, job_id):
        target_job = self._jobs.get(job_id)
        if not target_job:
            raise NoSuchJobException(job_id)
        return target_job.status


    def get_jobs_by_owner_id(self, owner_id):
        result = []
        for id, job in self._jobs.iteritems():
            if job.owner_id == owner_id:
                result.append(job)
        return result


    def get_all_jobs(self):
        result = []
        for id, job in self._jobs.iteritems():
            result.append(job)
        return result
