import os.path as osp
import jdlfactory


class Plugin:
    def entrypoint(self):
        raise NotImplementedError


class command(Plugin):
    """
    Plugin class for raw shell statements.
    """
    def __init__(self, cmds):
        if (jdlfactory.PY3 and isinstance(cmds, str)) or (jdlfactory.PY2 and isinstance(cmds, basestring)):
            cmds = [cmds]
        self.cmds = cmds
    
    def entrypoint(self):
        return self.cmds


python_env_debug_lines = [
    'echo ""',
    'echo "Summary of environment settings:"',
    'echo "  which python: $(which python)"',
    'echo "  python -V: $(python -V)"',
    'echo "  which python: $(which pip)"',
    'echo "  pip -V: $(pip -V)"',
    'echo "  PATH: ${PATH}"',
    'echo "  PYTHONPATH: ${PYTHONPATH}"',
    'echo "  PYTHONVERSION: ${PYTHONVERSION}"',
    ]

def create_pip_conf(path='${HOME}/.pip', venv_path='${HOME}/venv'):
    """
    Returns shell lines to create a pip.conf file at `path`.
    """
    conf_file = path.rstrip('/') + '/pip.conf'
    return [
        '',
        'echo "Creating .pip configuration file at {}"'.format(conf_file),
        'mkdir {}'.format(path),
        'cat <<EOF >> {}'.format(conf_file),
        # Here the pip settings
        '[global]',
        'prefix={}'.format(venv_path),
        '[install]',
        'no-cache-dir = true',
        'ignore-installed = true',
        # Done
        'EOF',
        'echo "Contents of {}:"'.format(conf_file),
        'cat {}'.format(conf_file),
        'echo END'
        ]

def manual_venv(path='${HOME}/venv'):
    """
    Creates a directory structure much like how `python -m venv` would create one:

    $HOME/venv/bin
    $HOME/venv/lib/python3.6/site-packages
    $HOME/venv/lib64/python3.6/site-packages

    and adds them to the path.
    Whatever is the current python executable is symlinked in $HOME/venv/bin.
    """
    return [
        'echo "Creating virtual env directory structure at %s and symlinking python"' % path,
        'mkdir -p %s/bin' % path,
        'ln -s $(which python) %s/bin/python' % path,
        'export PYTHONVERSION=$(python -c "import sys; print(\'{}.{}\'.format(sys.version_info.major, sys.version_info.minor))")',
        'mkdir -p %s/lib/python${PYTHONVERSION}/site-packages' % path,
        'mkdir -p %s/lib64/python${PYTHONVERSION}/site-packages' % path,
        # Activate:
        'export PATH="%s/bin:${PATH}"' % path,
        'export PYTHONPATH="%s/lib/python${PYTHONVERSION}/site-packages:%s/lib64/python${PYTHONVERSION}/site-packages:${PYTHONPATH}"' % (path, path)
        ]


class venv(Plugin):
    """
    Plugin to setup a virtual python environment in the job before actually
    running the worker_code.py file.

    For python3, it uses the '-m venv' method.

    python2 is more complicated: Most default job environments do not come
    with virtualenv or even pip installed. Thus for python2 this plugin 
    makes a manual virtual environment, creating the needed directory
    structure, downloading pip, and creating a .pip file to configure it.
    """

    def __init__(self, py3=False):
        self.py3 = py3

    @property
    def py2(self):
        return not self.py3

    def entrypoint(self):
        if self.py3:
            sh = [
                'command -v python3 >/dev/null 2>&1 || { echo >&2 "ERROR: python3 is not on the path!"; exit 1; }',
                'python3 -m venv venv',
                'source venv/bin/activate'
                ]
        else:
            sh = [
                'echo "Setting up a manual virtual environment for python 2"',
                'export HOME=$(pwd)',
                ]
            sh.extend(create_pip_conf())
            sh.extend(manual_venv())
            sh.extend([
                # Install pip - not available by default on most worker nodes
                'echo "Installing pip"',
                'mkdir tmppipinstalldir; cd tmppipinstalldir',
                'wget https://bootstrap.pypa.io/pip/${PYTHONVERSION}/get-pip.py',
                'python get-pip.py',
                'cd $HOME',
                ])
        sh.extend(python_env_debug_lines)
        return sh


class lcg(Plugin):
    """
    Plugin to start an environment based on the LCG setup scripts.
    Also sets up a directory structure for a venv.
    """
    def __init__(
        self,
        lcg_setup_script='/cvmfs/sft.cern.ch/lcg/views/LCG_98python3/x86_64-centos7-gcc9-opt/setup.sh',
        venv_path='${HOME}/venv',
        ):
        self.lcg_setup_script = lcg_setup_script
        self.venv_path = venv_path

    def entrypoint(self):
        sh = [
            'export HOME=$(pwd)',
            'echo "Sourcing {}"'.format(self.lcg_setup_script),
            'source {}'.format(self.lcg_setup_script),
            ]
        sh.extend(create_pip_conf(venv_path=self.venv_path))
        sh.extend(manual_venv(self.venv_path))
        sh.extend(python_env_debug_lines)
        return sh


class fix_gfal_env(Plugin):
    """
    Fixes the gfal-* command line utilities to the current environment.
    """
    def __init__(self, path='${PWD}/gfalbins', add_to_path=True, use_osg=False):
        self.path = path
        self.add_to_path = add_to_path
        self.use_osg = use_osg

    def entrypoint(self):
        # Make copies of environment variables in current state
        sh = [
            '',
            '#### GFAL ENV FIXES ####',
            'mkdir -p {}'.format(self.path),
            ]

        if self.use_osg:
            # Use a specific, hard-coded environment from the open science grid to run gfal tools
            osg_env = {
                'LD_LIBRARY_PATH' : '/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/lib64:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/lib:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/lib64:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/lib:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/lib64/dcap:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/lib64/lcgdm',
                'PYTHONPATH' : '/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/lib/python2.7/site-packages:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/lib64/python2.7/site-packages',
                'PATH' : '/cvmfs/cms.cern.ch/common:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/bin:/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/sbin:/cvmfs/cms.cern.ch/common:/usr/local/connect-client/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/puppetlabs/bin:/opt/dell/srvadmin/bin',
                }
        else:
            # Assume current environment variables work for gfal
            sh.extend([
                'export GFALPATH=$PATH',
                'export GFALPYTHONPATH=$PYTHONPATH',
                'export GFALLD_LIBRARY_PATH=$LD_LIBRARY_PATH',
                ])

        # For every gfal-* command line tool, overwrite it with a function:
        for tool in ['gfal-copy', 'gfal-rm', 'gfal-stat', 'gfal-ls', 'gfal-mkdir', 'gfal-cat']:
            new_bin_file = osp.join(self.path, tool)
            sh.extend([
                '',
                'echo "Creating new bin {} at {}"'.format(tool, new_bin_file),
                # Now write the contents of the new bin file
                'cat <<EOF >> {}'.format(new_bin_file),
                '#!/usr/bin/env bash',
                'unset PYTHONHOME',
                # Fix gfal to either the hard-coded osg version, or to the current environment
                'LD_LIBRARY_PATH=' + (osg_env['LD_LIBRARY_PATH'] if self.use_osg else '$GFALLD_LIBRARY_PATH'),
                'PYTHONPATH=' + (osg_env['PYTHONPATH'] if self.use_osg else '$GFALPYTHONPATH'),
                'PATH=' + (osg_env['PATH'] if self.use_osg else '$GFALPATH'),
                ('/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.6/3.6.230103-1/el7-x86_64/usr/bin/{} \\$@' if self.use_osg else '$(which {}) \\$@').format(tool),
                'EOF',
                # Done
                'chmod +x {}'.format(new_bin_file),
                'echo "Contents of {}:"'.format(new_bin_file),
                'cat {}'.format(new_bin_file),
                'echo ""',
                ])
        if self.add_to_path:
            sh.append('export PATH="{}:$PATH"'.format(self.path))
        return sh
