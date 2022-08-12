from __future__ import print_function

import os, os.path as osp, sys, copy, tempfile, shutil, json
from contextlib import contextmanager
from collections import OrderedDict


class Job(object):
    def __init__(self, data):
        self.data = data


class Group(object):
    def __init__(self, worker_code):
        self.worker_code = worker_code
        self.htcondor_settings = OrderedDict(        
            universe = 'vanilla',
            executable = 'entrypoint.sh',
            transfer_input_files = ['data.json', 'entrypoint.sh', 'worker_code.py']
            )
        self.jobs = []

    @property
    def njobs(self):
        return max(1, len(self.jobs))

    @property
    def jdl(self):
        jdl_str = ''
        for key, value in self.htcondor_settings.items():
            jdl_str += key + ' = '
            if isinstance(value, str):
                jdl_str += value
            elif isinstance(value, list):
                jdl_str += ','.join(value)
            jdl_str += '\n'
        jdl_str += 'queue ' + str(self.njobs)
        return jdl_str

    def json(self):
        return json.dumps(self, cls=CustomEncoder)

    def add_job(self, data):
        self.jobs.append(Job(data))

    def run_locally(self, ijob=0, keep_temp_dir=True):
        with simulated_job(self, keep_temp_dir, ijob):
            exec(self.worker_code)


@contextmanager
def simulated_job(group, keep_temp_dir=False, ijob=0):
    server_path = osp.join(osp.dirname(osp.abspath(__file__)), 'server')
    try:
        # Make sure jdlfactory_server is importable
        old_path = sys.path[:]
        sys.path.append(server_path)
        # Create the temporary directory representing the workdir of the job
        tmpdir = tempfile.mkdtemp('test')
        # Create the .job.ad file
        jobad_path = osp.join(tmpdir, '.job.ad')
        with open(jobad_path, 'w') as f:
            f.write(
                'ClusterId = 999999\n'
                'ProcId = {}\n'
                .format(ijob)
                )
        # Create the worker_code.py file
        with open(osp.join(tmpdir, 'worker_code.py'), 'w') as f:
            f.write(group.worker_code)
        # Create the data.json file
        with open(osp.join(tmpdir, 'data.json'), 'w') as f:
            json.dump(group, f, cls=CustomEncoder)
        # Set some environment variables htcondor would set in a job
        old_environ = copy.copy(os.environ)
        os.environ['_CONDOR_JOB_AD'] = jobad_path
        os.environ['_CONDOR_JOB_IWD'] = tmpdir
        # Change dir into the tmp dir
        old_cwd = os.getcwd()
        os.chdir(tmpdir)
        yield tmpdir
    finally:
        os.chdir(old_cwd)
        sys.path = old_path
        os.environ = old_environ
        if not keep_temp_dir: shutil.rmtree(tmpdir)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Group):
            return dict(
                worker_code = obj.worker_code,
                htcondor_settings = obj.htcondor_settings,
                jobs = [self.default(j) for j in obj.jobs]
                )
        elif isinstance(obj, Job):
            return dict(
                data = obj.data,
                )
        return json.JSONEncoder.default(self, obj)


def produce(worker_code):
    return Group(worker_code)
