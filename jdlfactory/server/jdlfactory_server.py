"""
This file contains all necessary python code for a remote job to run
"""
from __future__ import print_function
import os, os.path as osp, json


def get_job_ad(ad_file):
    ad = {}
    with open(ad_file, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            if not line: continue
            key, value = line.split(' = ', 1)
            ad[key] = value
    return ad

ad_file = os.environ['_CONDOR_JOB_AD']
iwd = os.environ['_CONDOR_JOB_IWD']


ad = get_job_ad(ad_file)
for key in ['ProcId', 'ClusterId']:
    if not key in ad:
        print('Warning: Could not find expected key {} in ad file {}'.format(key, ad_file))
clusterid = int(ad.get('ClusterId', -1))
ijob = int(ad.get('ProcId', -1))


data_json_file = osp.join(iwd, 'data.json')
with open(data_json_file, 'r') as f:
    group = json.load(f)


class DotDict(dict):
    """
    Small class that allows accessing keys via the dot (.) as well.
    Assumes data won't be changed.
    """
    def __init__(self, dct):
        super(DotDict, self).__init__(dct)
        self.__dict__.update(dct)

data = DotDict(group['jobs'][ijob]['data'])
group_data = DotDict(group['group_data'])