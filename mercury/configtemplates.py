#!/usr/bin/env python


DFPROC_TEMPLATE = '''
globals:

'''

EAVESDROPPR_TEMPLATE = '''
# 
# YAML init file for EavesdropPR listen/notify framework
#
#
globals:
        project_directory: {{ global_settings.data()['project_directory']}}
        database_host: {{ global_settings.data()['database_host'] }}
        database_name: {{ global_settings.data()['database_name'] }}
        debug: {{ global_settings.data()['debug']}}
        logfile: {{ global_settings.data()['logfile'] }}
        handler_module: {{ global_settings.data()['handler_module'] }}
        service_module: {{ global_settings.data()['service_module'] }} 
        

service_objects:
        {% for so in service_objects %}
        {{ so.name }}:
            class:
                {{ so.classname }}
            init_params:
                {% for p in so.init_params %}- name: {{ p['name'] }}
                  value: {{ p['value'] }}
                {% endfor %}
        {% endfor %}


channels:
        {% for ch in channels %}
        {{ch.name}}:
                handler_function: {{ch.handler_function}}
                db_table_name: {{ch.table_name}}
                db_operation: {{ch.operation}}
                pk_field_name: {{ch.primary_key_field}}
                pk_field_type: {{ch.primary_key_type}}
                db_schema: {{ch.schemaa}}
                db_proc_name: {{ch.procedure_name}}
                db_trigger_name: {{ch.trigger_name}}
                payload_fields:
                        {%for field in ch.payload_fields%}- {{field}}                        
                        {% endfor %}
        {% endfor %}
'''


XFILE_TEMPLATE = '''
globals:{% for gspec in project['globals'] %}  
  {{ gspec.name }}: {{ gspec.value }}{% endfor %}

service_objects:
{% for service in project['service_objects'] %}
  {{service.alias}}:
      class: {{service.classname}}
      init_params:
      {% for param in service.init_params %}
          - name: {{ param.name }}
            value: {{ param.value }}
      {% endfor %}{% endfor %}
sources:
{% for source in project['sources'] %}
  {{ source.name }}:
    class: {{ source.classname }}
{% endfor %}
maps:
{% for map in project['maps'] %}
  {{ map.name }}:
    lookup_source: {{ map.lookup_source}}
    settings: 
      - name: use_default_identity_transform
        value: True
    fields:
    {% for field in map.fields %}
      - {{ field.name }}:{% for p in field.parameters %}
          {{ p.name }}: {{ p.value }}{% endfor %}
    {% endfor %}
{% endfor %}
'''

NGST_TEMPLATE = '''
globals:{% for gspec in project['globals'] %}  
  {{ gspec.name }}: {{ gspec.value }}{% endfor %}

service_objects:
{% for service in project['service_objects'] %}
  {{service.alias}}:
      class: {{service.classname}}
      init_params:
      {% for param in service.init_params %}
          - name: {{ param.name }}
            value: {{ param.value }}
      {% endfor %}
{% endfor %}

datastores:
  {% for datastore in project.datastores %}
  {{ datastore.alias }}:
      class: {{ datastore.classname }}
      init_params:
        {% for param in datastore.params %}
        - name: {{ param.name }}
          value: {{ param.value }} {% endfor %}
      channel_select_function: {{ datastore.channel_detect_func }}
      channels:
        {% for channel in datastore.channels %}
        - {{ channel }} {% endfor %}
  {% endfor %}

ingest_targets:
  {% for target in project.ingest_targets %}
  {{ target.name }}:
      datastore: {{ target.datastore_alias }}
      checkpoint_interval: {{ target.checkpoint_interval }} {% endfor %}
'''

CYCLOPS_TEMPLATE = '''

'''

J2SQLGEN_TEMPLATE = '''
globals:{% for gspec in project['globals'] %}  
  {{ gspec.name }}: {{ gspec.value }}{% endfor %}

defaults:
    autocreate_pk_if_missing: {{ project.defaults.autocreate_pk }}
    pk_name: {{ project.defaults.pk_name }}
    pk_type: {{ project.defaults.pk_type }}
    varchar_length: {{ project.defaults.varchar_length }}

    column_type_map:

{% for mapping in project.column_type_map %}
    {{ mapping.source_type }}: {{ mapping.target_type }}    
{% endfor %}

tables:
    {% for table_map in project.table_maps %}
    {{ table_map.table_name }}:
        rename_to: {{ table_map.new_name }}
        column_settings:
            {% for %}
            {% endfor %}
        column_name_map:
            {% for column in table_map.renamed_columns %}
            {{ column.source_name }}: {{ column.target_name }}
            {% endfor %}
    {% endfor %}
'''

PGEXEC_TEMPLATE = '''
'''

PGMETA_TEMPLATE = '''
'''

PROFILR_TEMPLATE = '''
globals:{% for gspec in project['globals'] %}  
  {{ gspec.name }}: {{ gspec.value }}{% endfor %}

service_objects:
{% for service in services %}
  {{service.alias}}:
      class: {{service.classname}}
      init_params:
      {% for param in service.params %}
          - name: param.name
            value: param.value
      {% endfor %}
{% endfor %}

profilers:
{% for profiler in profilers %}
  {{profiler.alias}}:
    class: {{profiler.classname}}
{% endfor %}

datasets:
  {% for dataset in datasets %}
  {{dataset.name}}:
    profiler: {{dataset.profiler_name}}
    settings:

    tablename: {{dataset.table_name}}
    columns:
      {% for column in dataset.columns %}
      - {{column}}
      {% endfor %}
'''

QUASR_TEMPLATE = '''
globals:{% for gspec in project['globals'] %}  
  {{ gspec.name }}: {{ gspec.value }}{% endfor %}

service_objects:
{% for service in project['service_objects'] %}
  {{service.alias}}:
      class: {{service.classname}}
      init_params:
      {% for param in service.init_params %}
          - name: {{ param.name }}
            value: {{ param.value }}
      {% endfor %}{% endfor %}

templates:
  {% for t in project['templates'] %}
  {{t.name}}: |
    {{t.text}}
  {% endfor %}

jobs:
  {% for job in project['jobs'] %}
  {{ job.name }}:
    sql_template: {{ job.template_alias }}
    inputs:
      {% for slot in job.inputs %}
      - name: {{ slot.name }}
        type: {{ slot.datatype }}
      {% endfor %}
    outputs:
      {% for slot in job.outputs %}
      - name: {{ slot.name }}
        type: {{ slot.datatype }}
      {% endfor %}
    
    executor_function: {{ job.executor_function }}
    builder_function: {{ job.builder_function }}
    analyzer: {{ job.analyzer_function }}
  {% endfor %}
'''


