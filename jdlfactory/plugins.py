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
                # Create .pip configuration
                'echo "Creating .pip configuration file"',
                'mkdir $HOME/.pip',
                'echo "[global]" >  $HOME/.pip/pip.conf',
                'echo "prefix=${HOME}/venv" >>  $HOME/.pip/pip.conf',
                'echo "[install]"  >> $HOME/.pip/pip.conf',
                'echo "no-cache-dir = true" >> $HOME/.pip/pip.conf',
                'echo "ignore-installed = true" >> $HOME/.pip/pip.conf',
                # Setup directory structure, symlink in the python executable, and put it on the PATH
                'echo "Creating virtual env directory structure and symlinking python"',
                'mkdir -p $HOME/venv/bin',
                'ln -s $(which python) ${HOME}/venv/bin/python',
                'export PATH="${HOME}/venv/bin:${PATH}"',
                'export PYTHONVERSION=$(python -c "import sys; print(\'{}.{}\'.format(sys.version_info.major, sys.version_info.minor))")',
                'mkdir -p $HOME/venv/lib/python${PYTHONVERSION}/site-packages',
                'mkdir -p $HOME/venv/lib64/python${PYTHONVERSION}/site-packages',
                'export PYTHONPATH="${HOME}/venv/lib/python${PYTHONVERSION}/site-packages:${HOME}/venv/lib64/python${PYTHONVERSION}/site-packages:${PYTHONPATH}"',
                # Install pip
                'echo "Installing pip"',
                'mkdir tmppipinstalldir; cd tmppipinstalldir',
                'wget https://bootstrap.pypa.io/pip/${PYTHONVERSION}/get-pip.py',
                'python get-pip.py',
                'cd $HOME',
                ]
        # Printout some info for debugging purposes
        sh.extend([
            'echo "which python: $(which python)"',
            'echo "python -V: $(python -V)"',
            'echo "which python: $(which pip)"',
            'echo "pip -V: $(pip -V)"',
            'echo "PATH: ${PATH}"',
            'echo "PYTHONPATH: ${PYTHONPATH}"',
            'echo "PYTHONVERSION: ${PYTHONVERSION}"',
            ])
        return sh
