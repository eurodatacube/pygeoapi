# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#          Norman Barker <norman.barker@gmail.com>
#
# Copyright (c) 2020 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

""" Flask module providing the route paths to the api"""

import os

import click

from flask import Flask, Blueprint, make_response, request, send_from_directory

from pygeoapi.api import API
from pygeoapi.util import get_mimetype, yaml_load


CONFIG = None

if 'PYGEOAPI_CONFIG' not in os.environ:
    raise RuntimeError('PYGEOAPI_CONFIG environment variable not set')

with open(os.environ.get('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
    CONFIG = yaml_load(fh)

STATIC_FOLDER = 'static'
if 'templates' in CONFIG['server']:
    STATIC_FOLDER = CONFIG['server']['templates'].get('static', 'static')

APP = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='/static')
APP.url_map.strict_slashes = False

BLUEPRINT = Blueprint('pygeoapi', __name__, static_folder=STATIC_FOLDER)

# CORS: optionally enable from config.
if CONFIG['server'].get('cors', False):
    from flask_cors import CORS
    CORS(APP)

APP.config['JSONIFY_PRETTYPRINT_REGULAR'] = CONFIG['server'].get(
    'pretty_print', True)

api_ = API(CONFIG)

OGC_SCHEMAS_LOCATION = CONFIG['server'].get('ogc_schemas_location', None)

if (OGC_SCHEMAS_LOCATION is not None and
        not OGC_SCHEMAS_LOCATION.startswith('http')):
    # serve the OGC schemas locally

    if not os.path.exists(OGC_SCHEMAS_LOCATION):
        raise RuntimeError('OGC schemas misconfigured')

    @APP.route('/schemas/<path:path>', methods=['GET'])
    def schemas(path):
        """
        Serve OGC schemas locally

        :param path: path of the OGC schema document

        :returns: HTTP response
        """

        full_filepath = os.path.join(OGC_SCHEMAS_LOCATION, path)
        dirname_ = os.path.dirname(full_filepath)
        basename_ = os.path.basename(full_filepath)

        # TODO: better sanitization?
        path_ = dirname_.replace('..', '').replace('//', '')
        return send_from_directory(path_, basename_,
                                   mimetype=get_mimetype(basename_))


@BLUEPRINT.route('/')
def landing_page():
    """
    OGC API landing page endpoint

    :returns: HTTP response
    """
    headers, status_code, content = api_.landing_page(
        request.headers, request.args)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/openapi')
def openapi():
    """
    OpenAPI endpoint

    :returns: HTTP response
    """
    with open(os.environ.get('PYGEOAPI_OPENAPI'), encoding='utf8') as ff:
        openapi = yaml_load(ff)

    headers, status_code, content = api_.openapi(request.headers, request.args,
                                                 openapi)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/conformance')
def conformance():
    """
    OGC API conformance endpoint

    :returns: HTTP response
    """

    headers, status_code, content = api_.conformance(request.headers,
                                                     request.args)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections')
@BLUEPRINT.route('/collections/<collection_id>')
def collections(collection_id=None):
    """
    OGC API collections endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    headers, status_code, content = api_.describe_collections(
        request.headers, request.args, collection_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/queryables')
def collection_queryables(collection_id=None):
    """
    OGC API collections querybles endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_collection_queryables(
        request.headers, request.args, collection_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/items')
@BLUEPRINT.route('/collections/<collection_id>/items/<item_id>')
def collection_items(collection_id, item_id=None):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if item_id is None:
        headers, status_code, content = api_.get_collection_items(
            request.headers, request.args, collection_id)
    else:
        headers, status_code, content = api_.get_collection_item(
            request.headers, request.args, collection_id, item_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/coverage')
def collection_coverage(collection_id):
    """
    OGC API - Coverages coverage endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_collection_coverage(
        request.headers, request.args, collection_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/coverage/domainset')
def collection_coverage_domainset(collection_id):
    """
    OGC API - Coverages coverage domainset endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_collection_coverage_domainset(
        request.headers, request.args, collection_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/coverage/rangetype')
def collection_coverage_rangetype(collection_id):
    """
    OGC API - Coverages coverage rangetype endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_collection_coverage_rangetype(
        request.headers, request.args, collection_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/tiles')
def get_collection_tiles(collection_id=None):
    """
    OGC open api collections tiles access point

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_collection_tiles(
        request.headers, request.args, collection_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/tiles/<tileMatrixSetId>/metadata')  # noqa
def get_collection_tiles_metadata(collection_id=None, tileMatrixSetId=None):
    """
    OGC open api collection tiles service metadata

    :param collection_id: collection identifier
    :param tileMatrixSetId: identifier of tile matrix set

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_collection_tiles_metadata(
        request.headers, request.args, collection_id, tileMatrixSetId)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/collections/<collection_id>/tiles/\
<tileMatrixSetId>/<tileMatrix>/<tileRow>/<tileCol>')
def get_collection_tiles_data(collection_id=None, tileMatrixSetId=None,
                              tileMatrix=None, tileRow=None, tileCol=None):
    """
    OGC open api collection tiles service data

    :param collection_id: collection identifier
    :param tileMatrixSetId: identifier of tile matrix set
    :param tileMatrix: identifier of {z} matrix index
    :param tileRow: identifier of {y} matrix index
    :param tileCol: identifier of {x} matrix index

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_collection_tiles_data(
        request.headers, request.args, collection_id,
        tileMatrixSetId, tileMatrix, tileRow, tileCol)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/processes', methods=['GET', 'POST'])
@BLUEPRINT.route('/processes/<process_id>', methods=['GET', 'POST'])
def get_processes(process_id=None):
    """
    OGC API - Processes description endpoint

    :param process_id: process identifier

    :returns: HTTP response
    """
    # work around bug where json data is sent as form
    data = request.stream.read()
    if request.method == 'POST':
        if not process_id:
            raise NotImplementedError("Creating new processes is currently not supported")
        else:
            headers, status_code, content = api_.create_deferred_process(
                request.headers, request.args, data, process_id)
    else:
        headers, status_code, content = api_.describe_processes(
            request.headers, request.args, process_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response

@BLUEPRINT.route('/processes/<process_id>/deferred/<deferred_id>', methods=['GET', 'POST'])
def get_deferred_process(process_id, deferred_id):
    return api_.describe_deferred_process(
        process_id=process_id,
        deferred_process_id=deferred_id,
    )


@BLUEPRINT.route('/processes/<process_id>/deferred/<deferred_id>/coverage', methods=['GET', 'POST'])
def execute_deferred_process(process_id, deferred_id):
    return execute_coverage_process(process_id=deferred_id)


@BLUEPRINT.route('/processes/<process_id>/coverage', methods=['POST', 'GET'])
def execute_coverage_process(process_id):
    headers, status_code, content = api_.execute_coverage_process(
        headers=request.headers,
        args=request.args,
        data=request.data,
        process_id=process_id,
    )

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response

@BLUEPRINT.route('/processes/<process_id>/collection', methods=['GET'])
def get_coverage_collection_document(process_id):
    headers, status_code, content = api_.describe_coverage_process(process_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/processes/<process_id>/jobs', methods=['GET', 'POST'])
@BLUEPRINT.route('/processes/<process_id>/jobs/<job_id>',
                 methods=['GET', 'DELETE'])
def get_process_jobs(process_id=None, job_id=None):
    """
    OGC API - Processes jobs endpoint

    :param process_id: process identifier
    :param job_id: job identifier

    :returns: HTTP response
    """

    if job_id is None:
        if request.method == 'GET':  # list jobs
            headers, status_code, content = api_.get_process_jobs(
                request.headers, request.args, process_id)
        elif request.method == 'POST':  # submit job
            headers, status_code, content = api_.execute_process(
                request.headers, request.args, request.data, process_id)
    else:
        if request.method == 'DELETE':  # dismiss job
            headers, status_code, content = api_.delete_process_job(
                process_id, job_id)
        else:  # Return status of a specific job
            headers, status_code, content = api_.get_process_jobs(
                request.headers, request.args, process_id, job_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@APP.route('/processes/<process_id>/jobs/<job_id>/results', methods=['GET'])
def get_process_job_result(process_id=None, job_id=None):
    """
    OGC API - Processes job result endpoint

    :param process_id: process identifier
    :param job_id: job identifier

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_process_job_result(
        request.headers, request.args, process_id, job_id)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@APP.route('/processes/<process_id>/jobs/<job_id>/results/<resource>',
           methods=['GET'])
def get_process_job_result_resource(process_id, job_id, resource):
    """
    OGC API - Processes job result resource endpoint

    :param process_id: process identifier
    :param job_id: job identifier
    :param resource: job resource

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_process_job_result_resource(
        request.headers, request.args, process_id, job_id, resource)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/stac')
def stac_catalog_root():
    """
    STAC root endpoint

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_stac_root(
        request.headers, request.args)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@BLUEPRINT.route('/stac/<path:path>')
def stac_catalog_path(path):
    """
    STAC path endpoint

    :param path: path

    :returns: HTTP response
    """

    headers, status_code, content = api_.get_stac_path(
        request.headers, request.args, path)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


APP.register_blueprint(BLUEPRINT)


@click.command()
@click.pass_context
@click.option('--debug', '-d', default=False, is_flag=True, help='debug')
def serve(ctx, server=None, debug=False):
    """
    Serve pygeoapi via Flask. Runs pygeoapi
    as a flask server. Not recommend for production.

    :param server: `string` of server type
    :param debug: `bool` of whether to run in debug mode

    :returns: void
    """

    # setup_logger(CONFIG['logging'])
    APP.run(debug=True, host=api_.config['server']['bind']['host'],
            port=api_.config['server']['bind']['port'])


if __name__ == '__main__':  # run locally, for testing
    serve()
