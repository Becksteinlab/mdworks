from __future__ import unicode_literals

import subprocess
from fireworks import FireTaskBase


class GMXmdrunTask(FireTaskBase):
    _fw_name = "Gromacs mdrun task"
    required_params = ["s"]
    optional_params = ["modules"]

    def run_task(self, fw_spec):
        pass


class FileRsyncTask(FireTaskBase):
    """
    A FireTask to transfer files with rsync. Note that
    Required params:
        - mode: (str) - local, remotedest, remotesrc, remoteboth
        - files: ([str]) or ([(str, str)]) - list of source files or directories, or dictionary containing 'src' and 'dest' keys
        - dest: (str) destination directory, if not specified within files parameter
    Optional params:
        - srcuser: (str) remote user for src transfer
        - srchost: (str) remote host for src transfer
        - destuser: (str) remote user for dest transfer
        - desthost: (str) remote host for dest transfer
    """
    _fw_name = 'FileRsyncTask'
    required_params = ["mode", "files"]

    def run_task(self, fw_spec):
        shell_interpret = self.get('shell_interpret', True)
        ignore_errors = self.get('ignore_errors')
        mode = self.get('mode', 'local')

        returncodes = list()

        for f in self["files"]:

            if 'src' in f:
                src = f['src']
            else:
                src = f

            if mode == 'remotesrc' or mode == 'remoteboth':
                src = "{user}@{host}:{src}".format(user=self['srcuser'],
                                                    host=self['srchost'],
                                                    dest=src)

            if 'dest' in f:
                dest = f['dest']
            else:
                dest = self['dest']

            if mode == 'remotedest' or mode == 'remoteboth':
                src = "{user}@{host}:{dest}".format(user=self['destuser'],
                                                    host=self['desthost'],
                                                    dest=dest)

            returncodes.append(subprocess.call(["rsync", '-a', '--partial', src, dest], shell=True))

        if sum(returncodes) != 0:
            raise RuntimeError('FileRsyncTask fizzled! Return code: {}'.format(returncodes))
