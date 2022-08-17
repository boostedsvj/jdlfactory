import os, os.path as osp, tempfile, json, shutil, sys
from contextlib import contextmanager

import jdlfactory

if jdlfactory.PY3: from importlib import reload

def test_main_scope_server_attributes():
    group = jdlfactory.Group('print("Hello World!")')
    group.add_job(data=dict(foo='FOO'))
    group.group_data['mykey'] = 'myvalue'
    with jdlfactory.simulated_job(group, keep_temp_dir=True) as tmpdir:
        import jdlfactory_server as srv
        reload(srv) # If running multiple tests, python does not actually re-import unless prompted!

        print(srv)
        print(srv.__file__)
        print(srv.data_json_file)

        assert srv.ijob == 0
        assert srv.data == dict(foo='FOO')
        assert srv.data.foo == 'FOO'

        print(tmpdir)
        print(srv.group_data)
        import pprint
        pprint.pprint(srv.group_data)
        assert srv.group_data.mykey == 'myvalue'

