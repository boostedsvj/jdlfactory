from __future__ import print_function

import os, os.path as osp, sys, copy, tempfile, shutil, json, logging
from contextlib import contextmanager
from collections import OrderedDict


def setup_logger(name='jdlfactory'):
    if name in logging.Logger.manager.loggerDict:
        logger = logging.getLogger(name)
        logger.info('Logger %s is already defined', name)
    else:
        fmt = logging.Formatter(
            fmt = (
                '\033[34m[%(asctime)s:%(levelname)s:%(module)s:%(lineno)s]\033[0m'
                + ' %(message)s'
                ),
            datefmt='%Y-%m-%d %H:%M:%S'
            )
        handler = logging.StreamHandler()
        handler.setFormatter(fmt)
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    return logger
logger = setup_logger()


class Job(object):
    def __init__(self, data):
        self.data = data


class Group(object):
    def __init__(self, worker_code):
        self.worker_code = worker_code
        self.htcondor = OrderedDict(        
            universe = 'vanilla',
            executable = 'entrypoint.sh',
            transfer_input_files = ['jdlfactory_server.py', 'data.json', 'entrypoint.sh', 'worker_code.py'],
            output = "out_$(Cluster)_$(Process).txt",
            log = "htcondor.log",
            )
        self.jobs = []

    @property
    def njobs(self):
        return max(1, len(self.jobs))

    @property
    def jdl(self):
        jdl_str = ''
        for key, value in self.htcondor.items():
            jdl_str += key + ' = '
            if isinstance(value, str):
                jdl_str += value
            elif isinstance(value, list):
                jdl_str += ','.join(value)
            jdl_str += '\n'
        jdl_str += 'queue ' + str(self.njobs)
        return jdl_str

    def entrypoint(self):
        return (
            "#!/bin/bash\n"
            'echo "Redirecting stderr -> stdout from here on out"\n'
            "exec 2>&1\n"
            "set -e\n"
            'echo "hostname: $(hostname)"\n'
            'echo "date:     $(date)"\n'
            'echo "pwd:      $(pwd)"\n'
            'echo "ls -al:"\n'
            "ls -al\n"
            "\n"
            "export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch/\n"
            "source /cvmfs/cms.cern.ch/cmsset_default.sh\n"
            "python worker_code.py"
            )

    def json(self):
        return json.dumps(self, cls=CustomEncoder)

    def add_job(self, data):
        self.jobs.append(Job(data))

    def run_locally(self, ijob=0, keep_temp_dir=True):
        with simulated_job(self, keep_temp_dir, ijob):
            exec(self.worker_code)

    def prepare_for_jobs(self, rundir):
        if osp.isdir(rundir):
            raise Exception('Directory %s already exists!')
        os.makedirs(rundir)
        # Create the submit.jdl file
        with open(osp.join(rundir, 'submit.jdl'), 'w') as f:
            f.write(self.jdl)
        # Create the worker_code.py file
        with open(osp.join(rundir, 'entrypoint.sh'), 'w') as f:
            f.write(self.entrypoint())
        # Create the entrypoint.sh file
        with open(osp.join(rundir, 'worker_code.py'), 'w') as f:
            f.write(self.worker_code)
        # Create the data.json file
        with open(osp.join(rundir, 'data.json'), 'w') as f:
            json.dump(self, f, cls=CustomEncoder)
        # Copy the server file
        shutil.copyfile(
            osp.join(osp.dirname(osp.abspath(__file__)), 'server/jdlfactory_server.py'),
            osp.join(rundir, 'jdlfactory_server.py')
            )


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
                htcondor = obj.htcondor,
                jobs = [self.default(j) for j in obj.jobs]
                )
        elif isinstance(obj, Job):
            return dict(
                data = obj.data,
                )
        return json.JSONEncoder.default(self, obj)


def produce(worker_code):
    return Group(worker_code)
