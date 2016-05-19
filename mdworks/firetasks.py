from __future__ import unicode_literals

import os

from os.path import expanduser
from fireworks import FireTaskBase, FWAction


class BeaconTask(FireTaskBase):
    """
    A FireTask to tell the child Firework(s) where the generated files are so
    they can be pulled back down.

    """
    _fw_name = 'BeaconTask'

    def run_task(self, fw_spec):
        return FWAction(update_spec={'files': [os.environ['SCRATCHDIR']],
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
                        "There was an error performing operation {} from {} "
                        "to {}".format(mode, self["files"], self["dest"]))

        sftp.close()
        ssh.close()


class CleanupTask(FireTaskBase)
    """
    A FireTask for removing the directory and all files generated from an MD
    run.

    """
    _fw_name = 'CleanupTask'

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

        def delete_dir(sftp, directory):
            for g in sftp.listdir(directory):
                # first try to remove as a file
                try:
                    sftp.remove(os.path.join(directory, g))
                except OSError:
                    # must be a directory; first go inside and delete all files
                    delete_dir(os.path.join(directory, g))

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
                if not ignore_errors:
                    raise ValueError(
                        "There was an error deleting {} from {}".format(
                            item,fw_spec['server']))

        sftp.close()
        ssh.close()
