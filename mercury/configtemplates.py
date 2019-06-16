#!/usr/bin/env python


DFPROC_TEMPLATE = '''
globals:

'''

XFILE_TEMPLATE = '''
globals:
  project_home:                {{ project.home }}
  service_module:              {{ project.service_module }} 
  datastore_module:            {{ project.datastore_module }}

service_objects:
{% for service in config.services %}
  {{service.alias}}:
      class: {{service.classname}}
      init_params:
      {% for param in service.params %}
          - name: param.name
            value: param.value
      {% endfor %}
{% endfor %}

maps:
{% for map in config.maps%}
  {{ map.name }}:
    lookup_source: {{ map.lookup_source}}
    settings: 
      - name: use_default_identity_transform
              value: True
    fields:
      {% for field in map.fields %}
        - {{ field.name }}
          {% for p in field.parameters %}
          {{ p.name }}: {{ p.value }}
          {% endfor %}
      {% endfor %}
{% endfor %}}
'''

NGST_TEMPLATE = '''
globals:
  project_home:                {{ project.home }}
  service_module:              {{ project.service_module }} 
  datastore_module:            {{ project.datastore_module }}


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


datastores:
  {% for datastore in project.datastores %}
  {{ datastore.alias }}:
      channel_select_function: {{ datastore.channel_detect_func }}
      channels:
        {% for channel in datastore.channels %}
        - {{ channel }} {% endfor %} 
        
      class: {{ datastore.classname }}
      init_params:
        {% for param in datastore.params %}
        - name: {{ param.name }}
          value: {{ param.value }} {% endfor %}


ingest_targets:
  {% for target in project.ingest_targets %}
  {{ target.alias }}:
      datastore: {{ target.datastore_alias }}
      checkpoint_interval: {{ target.checkpoint_interval }} {% endfor %}
'''

CYCLOPS_TEMPLATE = '''

'''

J2SQLGEN_TEMPLATE = '''
globals:
    project_home: {{ project.home }}

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
globals:
  project_home: {project_home}
  profiler_module: {logic_module_name}  
  service_module: {service_module_name}

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
globals:
  project_home: {project_home}
  qa_logic_module: {logic_module_name}
  template_module: {template_module_name}
  service_module: {service_module_name}

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
templates:
  sample: |
    SELECT * FROM <TABLENAME>

jobs:
  {% for job in jobs %}
  {{ job.name }}:
    sql_template: {{ job.template_alias }}
    inputs:
      {% for slot in job.input_slots %}
      - name: {{ slot.name }}
        type: {{ slot.datatype }}
      {% endfor %}
    outputs:
      {% for slot in job.output_slots %}
      - name: {{ slot.name }}
        type: {{ slot.datatype }}
      {% endfor %}
      
    executor_function: {{ job.executor_function }}
    builder_function: {{ job.builder.function }}
    analyzer: {{ job.analyzer_function }}
'''


