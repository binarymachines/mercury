#!/usr/bin/env python

'''
Usage:
  mkcfg (xfile | ngst | cyclops | j2sqlgen | profilr | quasr)

'''

import os, sys
import docopt


NGST_TEMPLATE = '''

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


def main(args):
    pass


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)