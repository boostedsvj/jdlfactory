import os, os.path as osp, shutil, time, re, subprocess, glob
from contextlib import contextmanager

import pytest
import jdlfactory

@contextmanager
def capture_stdout():
    try:
        import sys
        if sys.version_info[0] == 2:
            from cStringIO import StringIO
        else:
            from io import StringIO
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        redirected_output = sys.stdout = sys.stderr = StringIO()
        yield redirected_output
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


def submit(jdl_file, wait=True):
    """
    Runs condor_submit on a jdl_file.
    If wait is True, also calls condor_wait on the logfile.
    """
    jdlfactory.logger.info('Running condor_submit %s', jdl_file)
    return_code = os.system('condor_submit %s 2>&1' % jdl_file)
    assert return_code == 0
    if wait:
        jdlfactory.logger.info('Sleeping 3 seconds, waiting for logfile to be created')
        time.sleep(3)
        jdlfactory.logger.info('Running condor_wait htcondor.log')
        return_code = os.system('condor_wait htcondor.log')
        jdlfactory.logger.info('condor_wait completed')
        jdlfactory.logger.info('return code of condor_wait htcondor.log: %s', return_code)        
        assert return_code == 0


def test_prepare_jobs():
    group = jdlfactory.Group('print("Hello World!")')
    try:
        group.prepare_for_jobs('helloworld')
        with open('helloworld/submit.jdl') as f:
            assert f.read() == (
                'executable = entrypoint.sh\n'
                'log = htcondor.log\n'
                'output = out_$(Cluster)_$(Process).txt\n'
                'transfer_input_files = jdlfactory_server.py,data.json,entrypoint.sh,worker_code.py\n'
                'universe = vanilla\n'
                'queue 1'
                )
        with open('helloworld/worker_code.py') as f:
            assert f.read() == 'print("Hello World!")'
        assert osp.isfile('helloworld/data.json')
        assert osp.isfile('helloworld/jdlfactory_server.py')
    finally:
        if osp.isdir('helloworld'): shutil.rmtree('helloworld')


@pytest.mark.realjobs
def test_run_basic_job():
    cwd = os.getcwd()
    group = jdlfactory.Group('print("Hello World!")')
    try:
        if osp.isdir('helloworld'): shutil.rmtree('helloworld')
        group.prepare_for_jobs('helloworld')
        os.chdir('helloworld')
        submit('submit.jdl', wait=True)
        with open(glob.glob('out_*.txt')[0]) as f:
            assert f.readlines()[-1] == 'Hello World!\n'
    finally:
        os.chdir(cwd)
        if osp.isdir('helloworld'): shutil.rmtree('helloworld')


@pytest.mark.realjobs
def test_run_jobs_with_different_data():
    cwd = os.getcwd()
    group = jdlfactory.Group('from jdlfactory_server import data; print(data.foo)')
    group.add_job(data=dict(foo='FOO'))
    group.add_job(data=dict(foo='BAR'))
    try:
        if osp.isdir('testjobs'): shutil.rmtree('testjobs')
        group.prepare_for_jobs('testjobs')
        os.chdir('testjobs')
        submit('submit.jdl', wait=True)
        outputs = []
        for outfile in sorted(glob.iglob('out_*.txt')):
            with open(outfile) as f:
                outputs.append(f.readlines()[-1])
        assert outputs == ['FOO\n', 'BAR\n']
    finally:
        os.chdir(cwd)
        if osp.isdir('testjobs'): shutil.rmtree('testjobs')


@pytest.mark.realjobs
def test_venv():
    group = jdlfactory.Group('import seutils; print(seutils)')
    group.venv()
    group.sh('pip install seutils')
    try:
        if osp.isdir('testjobs'): shutil.rmtree('testjobs')
        group.prepare_for_jobs('testjobs')
        os.chdir('testjobs')
        submit('submit.jdl', wait=True)
        with open(glob.glob('out_*.txt')[0]) as f:
            lastline = f.readlines()[-1]
        assert re.match(
            r"<module 'seutils' from '.*/venv/lib/python\d.\d/site-packages/seutils/__init__.pyc'>",
            lastline
            )
    finally:
        if osp.isdir('testjobs'): shutil.rmtree('testjobs')
