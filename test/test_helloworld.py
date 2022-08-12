from contextlib import contextmanager
import os, os.path as osp, json 

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
        redirected_output = sys.stdout = StringIO()
        yield redirected_output
    finally:
        sys.stdout = old_stdout



def test_hello_world():
    output = jdlfactory.produce('print("Hello World!")')
    assert output.jdl == (
        'universe = vanilla\n'
        'executable = entrypoint.sh\n'
        'transfer_input_files = data.json,entrypoint.sh,worker_code.py\n'
        'queue 1'
        )
    assert output.worker_code == 'print("Hello World!")'

def test_hello_foo():
    worker_code = (
        'from jdlfactory_server import data\n'
        'print("Hello {}!".format(data["foo"]))'
        )
    group = jdlfactory.Group(worker_code)
    group.add_job(data=dict(foo='FOO'))
    group.add_job(data=dict(foo='BAR'))

    assert group.jdl == (
        'universe = vanilla\n'
        'executable = entrypoint.sh\n'
        'transfer_input_files = data.json,entrypoint.sh,worker_code.py\n'
        'queue 2'
        )
    assert group.worker_code == worker_code
    assert group.jobs[0].data['foo'] == 'FOO'
    assert group.jobs[1].data['foo'] == 'BAR'


def test_json_encoding_job():
    json_encoded = json.dumps(jdlfactory.Job(dict(foo='bar')), cls=jdlfactory.CustomEncoder)
    assert json.loads(json_encoded) == dict(data=dict(foo='bar'))

def test_json_encoding_group():
    group = jdlfactory.Group('print("Hello World!")')
    group.add_job(data=dict(foo='FOO'))
    group.add_job(data=dict(foo='BAR'))
    json_encoded = json.dumps(group, cls=jdlfactory.CustomEncoder)
    assert json.loads(json_encoded) == dict(
        worker_code = group.worker_code,
        htcondor_settings = group.htcondor_settings,
        jobs = [dict(data=dict(foo='FOO')), dict(data=dict(foo='BAR'))]
        )


def test_simulated_job():
    group = jdlfactory.Group('print("Hello World!")')
    group.add_job(data=dict(foo='FOO'))

    with jdlfactory.simulated_job(group, keep_temp_dir=False) as tmpdir:
        import jdlfactory_server
        assert osp.isfile(osp.join(tmpdir, 'worker_code.py'))
        assert osp.isfile(osp.join(tmpdir, 'data.json'))


def test_group_run_locally():
    worker_code = (
        'from jdlfactory_server import data\n'
        'print("Hello {}!".format(data["foo"]))'
        )
    group = jdlfactory.Group(worker_code)
    group.add_job(data=dict(foo='FOO'))
    with capture_stdout() as captured:
        group.run_locally()
    assert captured.getvalue() == 'Hello FOO!\n'
