from __future__ import print_function

import os, os.path as osp, sys, copy, tempfile, shutil, json, logging, subprocess
from contextlib import contextmanager

PY3 = sys.version_info.major == 3
PY2 = sys.version_info.major == 2

def setup_logger(name='jdlfactory'):
    if name in logging.Logger.manager.loggerDict:
        logger = logging.getLogger(name)
        logger.info('Logger %s is already defined', name)
    else:
        fmt = logging.Formatter(
            fmt = (
                '\033[34m[%(name)s:%(levelname)s:%(asctime)s:%(module)s:%(lineno)s]\033[0m'
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
        self.data = copy.deepcopy(data)


class GroupBase(object):
    def __init__(self, worker_code):
        self.worker_code = worker_code
        self.htcondor = dict(
            universe = 'vanilla',
            executable = 'entrypoint.sh',
            transfer_input_files = ['data.json'],
            output = "out_$(Cluster)_$(Process).txt",
            log = "htcondor.log",
            )
        self.jobs = []
        self.plugins = []
        self.group_data = {}

    @classmethod
    def from_file(cls, filename):
        with open(filename, 'r') as f:
            return cls(f.read())

    @property
    def njobs(self):
        return max(1, len(self.jobs))

    @property
    def jdl(self):
        jdl_str = ''
        for key, value in sorted(self.htcondor.items()):
            jdl_str += key + ' = '
            if isinstance(value, str):
                jdl_str += value
            elif isinstance(value, list):
                jdl_str += ','.join(value)
            jdl_str += '\n'
        jdl_str += 'queue ' + str(self.njobs)
        return jdl_str

    def add_plugin(self, plugin):
        self.plugins.append(plugin)

    def venv(self, py3=False):
        self.add_plugin(plugins.venv(py3))

    def lcg(self, *args, **kwargs):
        self.add_plugin(plugins.lcg(*args, **kwargs))

    def sh(self, cmd):
        self.add_plugin(plugins.command(cmd))

    def json(self):
        return json.dumps(self, cls=CustomEncoder)

    def add_job(self, data):
        self.jobs.append(Job(data))

    def dump_job_files(self, dump_dir):
        if not osp.isdir(dump_dir):
            os.makedirs(dump_dir)
        # Create the data.json file
        with open(osp.join(dump_dir, 'data.json'), 'w') as f:
            json.dump(self, f, cls=CustomEncoder)
        # Copy the server file
        shutil.copyfile(
            osp.join(osp.dirname(osp.abspath(__file__)), 'server/jdlfactory_server.py'),
            osp.join(dump_dir, 'jdlfactory_server.py')
            )
        # Create the submit.jdl file
        with open(osp.join(dump_dir, 'submit.jdl'), 'w') as f:
            f.write(self.jdl)

    def prepare_for_jobs(self, rundir):
        if osp.isdir(rundir):
            raise Exception('Directory %s already exists!')
        os.makedirs(rundir)
        self.dump_job_files(rundir)


class Group(GroupBase):
    def __init__(self, worker_code):
        super(Group, self).__init__(worker_code)
        self.htcondor['transfer_input_files'].extend(['jdlfactory_server.py', 'entrypoint.sh', 'worker_code.py'])

    def entrypoint(self):
        sh = [
            "#!/bin/bash",
            'echo "Redirecting stderr -> stdout from here on out"',
            "exec 2>&1",
            "set -e",
            'echo "hostname: $(hostname)"',
            'echo "date:     $(date)"',
            'echo "pwd:      $(pwd)"',
            'echo "Initial ls -al:"',
            "ls -al",
            "export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch/",
            "source /cvmfs/cms.cern.ch/cmsset_default.sh",
            ]
        for plugin in self.plugins:
            sh.append("")
            sh.extend(plugin.entrypoint())
        sh.append("")        
        sh.append("python worker_code.py")
        return '\n'.join(sh)

    def dump_job_files(self, dump_dir):
        super(Group, self).dump_job_files(dump_dir)
        # Create the worker_code.py file
        with open(osp.join(dump_dir, 'entrypoint.sh'), 'w') as f:
            f.write(self.entrypoint())
        # Create the entrypoint.sh file
        with open(osp.join(dump_dir, 'worker_code.py'), 'w') as f:
            f.write(self.worker_code)

    def run_locally(self, ijob=0, keep_temp_dir=False):
        with simulated_job(self, keep_temp_dir, ijob) as tmpdir:
            return subprocess.check_output(['sh', 'entrypoint.sh'.format(tmpdir)])


class BashGroup(GroupBase):
    def __init__(self, worker_code):
        super(BashGroup, self).__init__(worker_code)
        self.htcondor['transfer_input_files'].extend(['script.sh'])

    def script(self):
        sh = [
            "#!/bin/bash",
            'echo "Redirecting stderr -> stdout from here on out"',
            "exec 2>&1",
            "set -e",
            'echo "hostname: $(hostname)"',
            'echo "date:     $(date)"',
            'echo "pwd:      $(pwd)"',
            'echo "Initial ls -al:"',
            "ls -al",
            "export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch/",
            "source /cvmfs/cms.cern.ch/cmsset_default.sh",
            ]
        for plugin in self.plugins:
            sh.append("")
            sh.extend(plugin.entrypoint())
        sh.append("")
        return '\n'.join(sh) + self.worker_code

    def dump_job_files(self, dump_dir):
        super(BashGroup, self).dump_job_files(dump_dir)
        # Create the worker_code.py file
        with open(osp.join(dump_dir, 'script.sh'), 'w') as f:
            f.write(self.script())

    def run_locally(self, ijob=0, keep_temp_dir=False):
        with simulated_job(self, keep_temp_dir, ijob) as tmpdir:
            return subprocess.check_output(['sh', 'script.sh'.format(tmpdir)])


@contextmanager
def simulated_job(group, keep_temp_dir=False, ijob=0, tag='_test'):
    old_cwd = os.getcwd()
    old_environ = copy.copy(os.environ)
    try:
        # Create the temporary directory representing the workdir of the job
        tmpdir = tempfile.mkdtemp(tag)
        logger.info('Simulating job in %s', tmpdir)
        group.dump_job_files(tmpdir)
        # Create the .job.ad file
        jobad_path = osp.join(tmpdir, '.job.ad')
        with open(jobad_path, 'w') as f:
            f.write(
                'ClusterId = 999999\n'
                'ProcId = {}\n'
                .format(ijob)
                )
        # Set some environment variables htcondor would set in a job
        os.environ['_CONDOR_JOB_AD'] = jobad_path
        os.environ['_CONDOR_JOB_IWD'] = tmpdir
        # Change dir into the tmp dir
        os.chdir(tmpdir)
        yield tmpdir
    finally:
        os.chdir(old_cwd)
        os.environ = old_environ
        if not keep_temp_dir: shutil.rmtree(tmpdir)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, GroupBase):
            return dict(
                worker_code = obj.worker_code,
                htcondor = obj.htcondor,
                jobs = [self.default(j) for j in obj.jobs],
                group_data = obj.group_data
                )
        elif isinstance(obj, Job):
            return dict(
                data = obj.data,
                )
        return json.JSONEncoder.default(self, obj)


def produce(worker_code):
    return Group(worker_code)

from . import plugins