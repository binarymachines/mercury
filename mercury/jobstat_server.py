#!/usr/bin/env python

#
# Generated Flask routing module for SNAP microservice framework
#



from flask import Flask, request, Response
from snap import snap
from snap import core
import json
import argparse
import sys

sys.path.append('/Users/dtaylor/workshop/Tictail/tdx')
import tdx_transforms

f_runtime = Flask(__name__)

if __name__ == '__main__':
    print 'starting SNAP microservice in standalone (debug) mode...'
    f_runtime.config['startup_mode'] = 'standalone'
    
else:
    print 'starting SNAP microservice in wsgi mode...'
    f_runtime.config['startup_mode'] = 'server'

app = snap.setup(f_runtime)
logger = app.logger
xformer = core.Transformer(app.config.get('services'))


#-- snap exception handlers ---

xformer.register_error_code(snap.NullTransformInputDataException, snap.HTTP_BAD_REQUEST)
xformer.register_error_code(snap.MissingInputFieldException, snap.HTTP_BAD_REQUEST)
xformer.register_error_code(snap.TransformNotImplementedException, snap.HTTP_NOT_IMPLEMENTED)

#------------------------------



#-- snap data shapes ----------


default_shape = core.InputShape("default_shape")

specify_pipeline_shape = core.InputShape("specify_pipeline_shape")
specify_pipeline_shape.add_field('id', True)

specify_pipeline_shape = core.InputShape("specify_pipeline_shape")
specify_pipeline_shape.add_field('id', True)

default_shape = core.InputShape("default_shape")


#------------------------------


#-- snap transform loading ----
xformer.register_transform('default', default_shape, tdx_transforms.default_func, 'application/json')
xformer.register_transform('list_ops', specify_pipeline_shape, tdx_transforms.list_ops_func, 'application/json')
xformer.register_transform('list_jobs', specify_pipeline_shape, tdx_transforms.list_jobs_func, 'application/json')
xformer.register_transform('list_pipelines', default_shape, tdx_transforms.list_pipelines_func, 'application/json')

#------------------------------



@app.route('/', methods=['GET'])
def default():
    try:        
        input_data = {}
        input_data.update(request.args)
        
        transform_status = xformer.transform('default', core.convert_multidict(input_data))        
        output_mimetype = xformer.target_mimetype_for_transform('default')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception, err:
        logger.error("Exception thrown: ", exc_info=1)        
        raise err


@app.route('/ops', methods=['GET'])
def list_ops():
    try:        
        input_data = {}
        input_data.update(request.args)
        
        transform_status = xformer.transform('list_ops', core.convert_multidict(input_data))        
        output_mimetype = xformer.target_mimetype_for_transform('list_ops')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception, err:
        logger.error("Exception thrown: ", exc_info=1)        
        raise err


@app.route('/jobs', methods=['GET'])
def list_jobs():
    try:        
        input_data = {}
        input_data.update(request.args)
        
        transform_status = xformer.transform('list_jobs', core.convert_multidict(input_data))        
        output_mimetype = xformer.target_mimetype_for_transform('list_jobs')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception, err:
        logger.error("Exception thrown: ", exc_info=1)        
        raise err


@app.route('/pipelines', methods=['GET'])
def list_pipelines():
    try:        
        input_data = {}
        input_data.update(request.args)
        
        transform_status = xformer.transform('list_pipelines', core.convert_multidict(input_data))        
        output_mimetype = xformer.target_mimetype_for_transform('list_pipelines')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception, err:
        logger.error("Exception thrown: ", exc_info=1)        
        raise err





if __name__ == '__main__':
    '''If we are loading from command line,
    run the Flask app explicitly
    '''
    
    app.run(port=5000)


