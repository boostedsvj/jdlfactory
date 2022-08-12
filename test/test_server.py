import os, os.path as osp, tempfile, json, shutil, sys
from contextlib import contextmanager

import jdlfactory

def test_main_scope_server_attributes():
    group = jdlfactory.Group('print("Hello World!")')
    group.add_job(data=dict(foo='FOO'))
    with jdlfactory.simulated_job(group, keep_temp_dir=False) as tmpdir:
        import jdlfactory_server as srv
        assert srv.ijob == 0
        assert srv.data == dict(foo='FOO')
        assert srv.data.foo == 'FOO'


