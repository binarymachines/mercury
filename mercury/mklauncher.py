#!/usr/bin/env python

''' Usage: mklauncher.py
'''



import docopt
import jinja2
import os
from snap import common


TEMPLATE_FILE = 'metl_launcher.sh.j2'


def main(args):

    j2env = jinja2.Environment(loader = jinja2.FileSystemLoader(os.getcwd()))
    template_mgr = common.JinjaTemplateManager(j2env)

    launcher_data = dict(tdx_home = os.getenv('METL_HOME'),
                         s3_bucket = 'bucket_name')

    launcher_template = template_mgr.get_template(TEMPLATE_FILE)    
    output_data = launcher_template.render(launcher_data)

    print(output_data)

    
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
