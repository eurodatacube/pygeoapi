# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2019 Tom Kralidis
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

from datetime import datetime
import io
import json
import logging
from multiprocessing import dummy
import os

from pygeoapi.util import JobStatus

LOGGER = logging.getLogger(__name__)
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

class BaseManager(object):
    """generic Manager ABC"""

    def __init__(self, manager_def):
        """
        Initialize object

        :param manager_def: manager definition

        :returns: `pygeoapi.process.manager.base.BaseManager`
        """

        self.name = manager_def['name']
        self.output_dir = manager_def.get('output_dir', '/tmp')

    def create(self):
        """
        Create manager

        :returns: `bool` status of result
        """

        raise NotImplementedError()

    def destroy(self):
        """
        Destroy manager

        :returns: `bool` status of result
        """

        raise NotImplementedError()

    def get_jobs(self, processid=None, status=None):
        """
        Get jobs

        :param processid: process identifier
        :param status: job status (accepted, running, successful,
                       failed, results) (default is all)

        :returns: list of jobs (identifier, status, process identifier)
        """

        raise NotImplementedError()

    def get_job_result(self, processid, jobid):
        """
        Get a single job

        :param processid: process identifier
        :param jobid: job identifier

        :returns: `dict`  # `pygeoapi.process.manager.Job`
        """

        raise NotImplementedError()

    def add_job(self, job_metadata):
        """
        Add a job

        :param job_metadata: `dict` of job metadata

        :returns: add job result
        """

        raise NotImplementedError()

    def update_job(self, processid, job_id, update_dict):
        """
        Updates a job

        :param processid: process identifier
        :param job_id: job identifier
        :param update_dict: `dict` of property updates

        :returns: `bool` of status result
        """

        raise NotImplementedError()

    def _execute_handler_async(self, p, job_id, data_dict):
        """
        This private execution handler executes a process in a background thread
        using multiprocessing.dummy
        https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.dummy

        :param p: `pygeoapi.process` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters

        :returns: tuple of None (i.e. initial response payload)
                  and JobStatus.accepted (i.e. initial job status)
        """
        _process = dummy.Process(
            target=self._execute_handler,
            args=(p, job_id, data_dict)
        )
        _process.start()
        return None, JobStatus.accepted

    def _execute_handler(self, p, job_id, data_dict):
        """
        This private exeution handler writes output to disk as a process output
        store. There is no clean-up of old process outputs.

        :param p: `pygeoapi.process` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters

        :returns: tuple of response payload and status
        """
        filename = '{}-{}'.format(p.metadata['id'], job_id)
        job_filename = os.path.join(self.output_dir, filename)

        processid = p.metadata['id']
        current_status = JobStatus.accepted
        job_metadata = {
            'identifier': job_id,
            'processid': processid,
            'process_start_datetime': datetime.utcnow().strftime(DATETIME_FORMAT),
            'process_end_datetime': None,
            'status': current_status.value,
            'location': None,
            'message': 'Job accepted and ready for execution',
            'progress': 5
        }
        self.add_job(job_metadata)

        try:
            current_status = JobStatus.running
            outputs = p.execute(data_dict)
            self.update_job(processid, job_id, {
                'status': current_status.value,
                'message': 'Writing job output',
                'progress': 95
            })

            with io.open(job_filename, 'w') as fh:
                fh.write(json.dumps(outputs, sort_keys=True, indent=4))

            current_status = JobStatus.finished
            job_update_metadata = {
                'process_end_datetime': datetime.utcnow().strftime(DATETIME_FORMAT),
                'status': current_status.value,
                'location': job_filename,
                'message': 'Job complete',
                'progress': 100
            }

            self.update_job(processid, job_id, job_update_metadata)

        except Exception as err:
            LOGGER.exception("error during execution")
            # TODO assess correct exception type and description to help users
            # NOTE, the /results endpoint should return the error HTTP status
            # for jobs that failed, ths specification says that failing jobs
            # must still be able to be retrieved with their error message
            # intact, and the correct HTTP error status at the /results
            # endpoint, even if the /result endpoint correctly returns the
            # failure information (i.e. what one might assume is a 200
            # response).
            current_status = JobStatus.failed
            code = 'InvalidParameterValue'
            status_code = 400
            outputs = {
                'code': code,
                'description': str(err) # NOTE this is optional and internal exceptions aren't useful for (or safe to show) end-users
            }
            LOGGER.error(outputs)
            job_metadata = {
                'process_end_datetime': datetime.utcnow().strftime(DATETIME_FORMAT),
                'status': current_status.value,
                'location': None,
                'message': f'{code}: {outputs["description"]}'
            }

            self.update_job(processid, job_id, job_metadata)

        return outputs, current_status

    def execute_process(self, p, job_id, data_dict, sync=True):
        """
        Default process execution handler

        :param p: `pygeoapi.process` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters
        :param sync: `bool` specifying sync or async processing.

        :returns: tuple of response payload and status
        """
        if sync:
            LOGGER.debug('Synchronous execution')
            return self._execute_handler(p, job_id, data_dict)

        LOGGER.debug('Asynchronous execution')
        return self._execute_handler_async(p, job_id, data_dict)

    def get_job_status(self, p, job_id, data_dict):
        """

        """

    def get_job_output(self, processid, job_id):
        """
        Returns the actual output from a finished process, or else None if the
        process has not finished execution.

        :param processid: process identifier
        :param job_id: job identifier

        :returns: tuple of: JobStatus `Enum`, and
        """
        #TODO doc: what is the return type?

    def delete_job(self, processid, job_id):
        """
        Deletes a job

        :param processid: process identifier
        :param job_id: job identifier

        :returns: `bool` of status result
        """

        raise NotImplementedError()

    def delete_jobs(self, max_jobs, older_than):
        """
        TODO
        """

        raise NotImplementedError()

    def __repr__(self):
        return '<ManagerProcessor> {}'.format(self.name)


class ManagerExecuteError(Exception):
    """query / backend error"""
    pass
