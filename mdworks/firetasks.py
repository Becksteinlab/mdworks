from __future__ import unicode_literals

from six import string_types
import os

import traceback
from os.path import expandvars, expanduser, abspath
from fireworks import FireTaskBase, FWAction


class StagingTask(FireTaskBase):
    """
    A FireTask to stage files on remote resources.

    Required params:
        - stages: (list) - Dicts giving for each of the following keys:
                - 'server': server host to transfer to
                - 'user': username to authenticate with
                - 'staging': absolute path to staging area on remote resource
            alternatively, a path to a yaml file giving a list of dictionaries
            with the same information.
        - files: ([str]) - full paths to files to stage
        - uuid: (str) = uuid of Sim to stage files for
    Optional params:
        - key_filename: (str) optional SSH key location for remote transfer
        - shell_interpret: (bool) - if True (default) interpret local paths to files with a shell; allows variables and e.g. `~`
        - allow_missing: (bool) - if False (default), raise an error if one of the files to stage is not present
        - max_retry: (int) - number of times to retry failed transfers; defaults to `0` (no retries)
        - retry_delay: (int) - number of seconds to wait between retries; defaults to `10`

    """
    _fw_name = 'StagingTask'
    required_params = ["stages", "files", "uuid"]

    def run_task(self, fw_spec):
        import yaml
        import time
        import paramiko

        files = self.get('files')
        stages = self.get('stages')

        shell_interpret = self.get('shell_interpret', True)
        max_retry = self.get('max_retry', 0)
        allow_missing = self.get('allow_missing', False)
        retry_delay = self.get('retry_delay', 10)
        mode = self.get('mode', 'move')

        # we don't want an error raised for missing files, so we remove them.
        # perhaps change with a try-except instead
        if allow_missing:
            files = [f for f in files if os.path.exists(f)]

        # if stages is a path, we load the stages info from the file
        if isinstance(stages, string_types):
            with open(stages, 'r') as f:
                stages = yaml.load(f)

        # send the files to each stage
        for stage in stages:
            # we place files in a staging directory corresponding to its uuid
            # easy to find that way
            dest = os.path.join(stage['staging'], self['uuid'])
            
            # create ssh connection
            ssh = paramiko.SSHClient()
            ssh.load_host_keys(expanduser(os.path.join("~", ".ssh", "known_hosts")))
            ssh.connect(stage['server'], username=stage['user'], key_filename=self.get('key_filename'))
            sftp = ssh.open_sftp()

            # if destination exists, delete all files inside; don't want stale files
            if self._rexists(sftp, dest):
                for f in sftp.listdir(dest):
                    sftp.remove(os.path.join(dest, f))
            else:
                sftp.mkdir(dest)

            for f in self["files"]:
                try:
                    src = abspath(expanduser(expandvars(f))) if shell_interpret else f

                    # we don't want an error raised for missing files, so we
                    # skip them if allow_missing is True
                    if allow_missing and not os.path.exists(src):
                        continue

                    sftp.put(src, os.path.join(dest, os.path.basename(src)))
                except:
                    traceback.print_exc()
                    if max_retry:
                        # we want to avoid hammering either the local or remote machine
                        time.sleep(retry_delay)
                        self['max_retry'] -= 1
                        self.run_task(fw_spec)
                    else:
                        raise

            sftp.close()
            ssh.close()

            # give the ssh daemon some time to breathe between stagings;
            # with many simulations may be staging many simulations at once
            # from same server
            time.sleep(5)

    def _rexists(self, sftp, path):
        """
        os.path.exists for paramiko's SCP object
        """
        try:
            sftp.stat(path)
        except IOError as e:
            if e[0] == 2:
                return False
            raise
        else:
            return True


class Stage2RunDirTask(FireTaskBase):
    """
    A FireTask to make the rundir for an MD run, and copy files from staging.

    Required params:
        - uuid: (str) uuid of Sim to make rundir for

    """
    _fw_name = 'Stage2RunDirTask'
    required_params = ["uuid"]

    def run_task(self, fw_spec):
        import shutil

        rundir = os.path.join(os.environ['SCRATCHDIR'], self['uuid'])
        staging = os.path.join(os.environ['STAGING'], self['uuid'])

        try:
            os.makedirs(rundir)
        except OSError:
            # we don't care if the directory already exists
            pass

        # copy files from stage to rundir
        for f in os.listdir(staging):
            shutil.copy(os.path.join(staging, f), rundir)


class BeaconTask(FireTaskBase):
    """
    A FireTask to tell the child Firework(s) where the generated files are so
    they can be pulled back down.

    Required params:
        - uuid: (str) uuid of Sim to set beacon for

    """
    _fw_name = 'BeaconTask'
    required_params = ["uuid"]

    def run_task(self, fw_spec):
        return FWAction(update_spec={'files': [os.path.join(os.environ['SCRATCHDIR'], self['uuid'])],
                                     'server': os.environ['HOST'],
                                     'user': os.environ['USER']})


class FilePullTask(FireTaskBase):
    """
    A FireTask to pull files from a remote server. Uses information from a
    BeaconTask for determining where to pull from.
    
    Required params:
        - dest: (str) destination directory, if not specified within files parameter
    Optional params:
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
        ssh.connect(fw_spec['server'], username=fw_spec['user'], key_filename=self.get('key_filename'))
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
                        "There was an error performing pull from {} "
                        "to {}".format(self["files"], self["dest"]))

        sftp.close()
        ssh.close()


class CleanupTask(FireTaskBase):
    """
    A FireTask for removing the directory and all files generated from an MD
    run.

    """
    _fw_name = 'CleanupTask'
    required_params = ["uuid"]

    def run_task(self, fw_spec):
        shell_interpret = self.get('shell_interpret', True)
        ignore_errors = self.get('ignore_errors')

        # Create SFTP connection
        import paramiko
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(expanduser(os.path.join("~", ".ssh", "known_hosts")))
        ssh.connect(fw_spec['server'], username=fw_spec['user'], key_filename=self.get('key_filename'))
        sftp = ssh.open_sftp()

        def delete_dir(sftp, directory):
            for g in sftp.listdir(directory):
                # first remove files
                sftp.remove(os.path.join(directory, g))

            # then delete the directory
            sftp.rmdir(directory)

        for item in fw_spec["files"]:
            try:
                # try case where item is a directory
                try:
                    delete_dir(sftp, item)
                except IOError:
                    # if src isn't a directory, it should be a file
                    sftp.remove(item)
            except:
                traceback.print_exc()
                raise

        sftp.close()
        ssh.close()
