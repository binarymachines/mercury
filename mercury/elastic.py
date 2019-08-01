#!/usr/bin/env python

import os, sys
import json
import uuid
import requests
from snap import common
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, connections


ElasticsearchHost = namedtuple('ElasticsearchHost', 'host port')


class ESDatabase(object):
    def __init__(self, service_registry, **kwargs):
        kwreader = common.KeywordArgReader('hostname', 'port')
        self.host = kwargs['hostname']
        self.port = int(kwargs['port'])
        self.esclient = Elasticsearch([{'host': self.host, 'port': self.port}])


    def generate_doc_id(self):
        return uuid.uuid4()


    def create_index(self, index_name):
        # ignore if index already exists
        return self.esclient.indices.create(index=index_name, ignore=400)


    def index_record(self, index_name, doctype, **payload):
        self.esclient.index(index=index_name,
                            doc_type=doctype,
                            id=self.generate_doc_id(),
                            body=payload)

    def query(self, index_name, query_dict, **kwargs):
        payload = {}
        payload['index'] = index_name
        payload['body'] = query_dict
        payload.update(kwargs)

        return self.esclient.search(**payload)  
 