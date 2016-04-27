from __future__ import unicode_literals

import subprocess
from fireworks import FireTaskBase


class GMXmdrunTask(FireTaskBase):
    _fw_name = "Gromacs mdrun task"
    required_params = ["s"]
    optional_params = ["modules"]

    def run_task(self, fw_spec):
        pass


class FilePull(FireTaskBase):
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
    _fw_name = 'PullTask'
    required_params = ["files"]

    def run_task(self, fw_spec):
        shell_interpret = self.get('shell_interpret', True)
        ignore_errors = self.get('ignore_errors')

        # remote transfers
        # Create SFTP connection
        import paramiko
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(expanduser(os.path.join("~", ".ssh", "known_hosts")))
        ssh.connect(self['server'], username=self.get('user'), key_filename=self.get['key_filename'])
        sftp = ssh.open_sftp()

        for src in self["files"]:
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
