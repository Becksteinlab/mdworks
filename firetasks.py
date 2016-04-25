
from fireworks import FireTaskBase


class GMXmdrunTask(FireTaskBase):
    _fw_name = "Gromacs mdrun task"
    required_params = ["s"]
    optional_params = ["modules"]

    def run_task(self, fw_spec):
        smaller = self['smaller']
        larger = self['larger']

        m_sum = smaller + larger
        if m_sum < stop_point:
            print('The next Fibonacci number is: {}'.format(m_sum))
            # create a new Fibonacci Adder to add to the workflow
            new_fw = Firework(GMXmdrunTask(), {'smaller': larger, 'larger': m_sum, 'stop_point': stop_point})
            return FWAction(stored_data={'next_fibnum': m_sum}, additions=new_fw)

        else:
            print('We have now exceeded our limit; (the next Fibonacci number would have been: {})'.format(m_sum))
            return FWAction()


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

            returncodes.append(subprocess.call(["bash", "rsync", src, dest]))

        if sum(returncodes) != 0:
            raise RuntimeError('FileRsyncTask fizzled! Return code: {}'.format(returncodes))
