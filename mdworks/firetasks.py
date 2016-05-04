from __future__ import unicode_literals

import os
import warnings

from os.path import expanduser
import subprocess
from fireworks import FireTaskBase, FWAction


class GMXmdrunTask(FireTaskBase):
    _fw_name = "Gromacs mdrun task"
    required_params = ["s"]
    optional_params = ["modules"]

    def run_task(self, fw_spec):
        pass


class BeaconTask(FireTaskBase):
    """
    A FireTask to tell the next Firework where the generated files are
    so they can be pulled back down.

    """
    _fw_name = 'BeaconTask'

    def run_task(self, fw_spec):
        return FWAction(update_spec={'files': [os.environ['SCRATCHDIR']],
                                     'server': os.environ['HOST'],
                                     'user': os.environ['USER']})


class FilePullTask(FireTaskBase):
    """
    A FireTask to Transfer files from a remote server. Note that
    Required params:
        - files: ([str]) - list of source files or directories
        - dest: (str) destination directory, if not specified within files parameter
    Optional params:
        - server: (str) server host for source files
        - user: (str) user to authenticate with on remote server
        - key_filename: (str) optional SSH key location for remote transfer
    """
    _fw_name = 'FilePullTask'
    required_params = ["dest"]

    def run_task(self, fw_spec):
        shell_interpret = self.get('shell_interpret', True)
        ignore_errors = self.get('ignore_errors')

        # remote transfers
        # Create SFTP connection
        import paramiko
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(expanduser(os.path.join("~", ".ssh", "known_hosts")))
        ssh.connect(fw_spec['server'], username=self.get('user'), key_filename=self.get('key_filename'))
        sftp = ssh.open_sftp()

        for src in fw_spec["files"]:
            try:
                dest = self['dest']

                # make destination if it doesn't exist already
                if not os.path.exists(dest):
                    os.mkdirs(dest)

                # try case where src is a directory
                try:
                    for g in sftp.listdir(src):
                        sftp.get(os.path.join(src, g), os.path.join(dest, g))
                except IOError:
                    # if src isn't a directory, it should be a file
                    sftp.get(src, os.path.join(dest, os.path.basename(src)))

            except:
                traceback.print_exc()
                if not ignore_errors:
                    raise ValueError(
                        "There was an error performing operation {} from {} "
                        "to {}".format(mode, self["files"], self["dest"]))

        sftp.close()
        ssh.close()


class ContinueTask(FireTaskBase):
    """
    A FireTask to check the step listed in the CPT file against the total
    number of steps desired in the TPR file. If there are steps left to
    go, another MD workflow is submitted.

    Parameters
    ----------
    sim : str
        MDSynthesis Sim.
    archive : str
        Absolute path to directory to launch from, which holds all required data.
    stages: list
        Dicts giving for each of the following keys:
            - 'server': server host to transfer to
            - 'user': username to authenticate with
            - 'staging': absolute path to staging area on remote resource
    postprocessing_wf : Workflow
        Workflow to perform after copyback; performed in parallel to continuation run.
    tpr : str
        File name (not path) of run-input file.
    cpt : str
        File name (not path) of checkpoint file; need not exist.

    """
    _fw_name = 'ContinueTask'
    required_params = ["sim",
                       "archive",
                       "stages",
                       "md_category",
                       "postprocessing_wf",
                       "tpr",
                       "cpt"]

    def run_task(self, fw_spec):
        from .interactive import make_md_workflow

        import gromacs

        cpt = os.path.join(self['archive'], self['cpt'])
        tpr = os.path.join(self['archive'], self['tpr'])

        # extract step number from CPT file
        out = gromacs.dump(cp=cpt, stdout=False)
        step = int([line.split(' ')[-1] for line in out[1].split('\n') if 'step = ' in line][0])

        # extract nsteps from TPR file
        out = gromacs.dump(s=tpr, stdout=False)
        nsteps = int([line.split(' ')[-1] for line in out[1].split('\n') if 'nsteps' in line][0])

        # if step < nsteps, we submit a new workflow
        if step < nsteps:
            wf = make_md_workflow(sim=self['sim'],
                                  archive=self['archive'],
                                  stages=self['stages'],
                                  md_category=self['md_category'],
                                  postprocessing_wf=self['postprocessing_wf'],
                                  tpr=self['tpr'],
                                  cpt=self['cpt'])

            return FWAction(additions=wf)


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
