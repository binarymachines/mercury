#!/usr/bin/env python

 
from snap import common
import datamap


class PipelineSpec(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('staging_topic',
                                           'core_topic',
                                           'kafka_cluster',
                                           'staging_record_transformer', 
                                           'core_record_transformer')

        kwreader.read(**kwargs)
        self._staging_topic = kwargs['staging_topic']
        self._core_topic = kwargs['core_topic']
        self._cluster = kwargs['kafka_cluster']
        self._staging_transformer = kwargs['staging_record_transformer']
        self._core_transformer = kwargs['core_record_transformer']


    def get_user_defined_consumer_group(self, alias):
        cgroup = self._user_defined_consumer_groups.get(alias)
        if not cgroup:
            #TODO: create custom exception
            raise Exception('No consumer group with alias "%s" registered in pipeline config' % alias)
        return cgroup


    @property
    def staging_topic(self):
        return self._staging_topic


    @property
    def core_topic(self):
        return self._core_topic


    @property
    def cluster(self):
        return self._cluster


    @property
    def topic_aliases(self):
        return self._user_topics.keys()    
    

    @property
    def group_aliases(self):
        return self._user_defined_consumer_groups.keys()


    def get_user_defined_topic(self, alias):
        topic = self._user_topics.get(alias)
        if not topic:
            # TODO: create custom exception
            raise Exception('No topic with alias "%s" registered in pipeline spec' % alias)
        return topic

    
    @property
    def staging_transformer(self):
        return self._staging_transformer


    @property
    def core_transformer(self):
        return self._core_transformer


    
class PipelineSpecBuilder(object):
    def __init__(self, yaml_config, **kwargs):
        self._yaml_config = yaml_config


    def set_staging_transformer(self, **kwargs):
        kwreader = common.KeywordArgReader('map_name', 
                                           'config_file')
        kwreader.read(**kwargs)
        yaml_config_file = kwargs['config_file']
        map_anme = kwargs['map_name']

        builder = datamap.RecordTransformerBuilder(yaml_config_file, map_name=map_name)
        self.staging_transformer = builder.build()
        return self


    def set_core_transformer(self, map_name, yaml_config_file):
        kwreader = common.KeywordArgReader('map_name', 
                                           'config_file')
        kwreader.read(**kwargs)
        yaml_config_file = kwargs['config_file']
        map_anme = kwargs['map_name']

        builder = datamap.RecordTransformerBuilder(yaml_config_file, map_name=map_name)
        self.core_transformer = builder.build()
        return self


    def build(self):
        user_topics = {}
        user_defined_consumer_groups = {}
        file_references = {}

        cluster = KafkaCluster()
        for entry in self._yaml_config['globals']['cluster_nodes']:
            tokens = entry.split(':')
            ip = tokens[0]
            port = tokens[1]
            cluster = cluster.add_node(KafkaNode(ip, port))

        t_staging = yaml_config['staging_topic']
        t_core = yaml_config['core_topic']

        if self._yaml_config.get('user_topics'):
            for entry in self._yaml_config['user_topics']:
                user_topics[entry['alias']] = entry['name']

        if self._yaml_config.get('user_defined_consumer_groups'):
            for entry in self._yaml_config['user_defined_consumer_groups']:
                user_defined_consumer_groups[entry['alias']] = entry['name']

        pipeline_spec = PipelineSpec(staging_topic=t_staging,
                                     core_topic=t_core,
                                     kafka_cluster=cluster,
                                     staging_transform=self._staging_transformer,
                                     core_transform=self.core_transformer)

        return pipeline_spec        



 