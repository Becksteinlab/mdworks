import os

from fireworks import ScriptTask, PyTask
from fireworks.core.firework import Firework

from fireworks.features.background_task import BackgroundTask

from .firetasks import GMXmdrunTask, FileRsyncTask


def make_md_firework(name, launchdir, scratchdir, deffnm='md', tpr='md.tpr',
                     cpt='md.cpt', srcuser=None, srchost=None):
    """Construct an MD Firework. This firework can then be incorporated into a
    workflow and submitted to a launchpad.

    Parameters
    ----------
    name : str
        Name to give this Firework.
    launchdir : str
        Absolute path to directory to launch from, which holds all required and
        generated data.
    scratchdir : str
        Absolute path to directory on remote resource to run MD within.
    deffnm : str
        Prefix for all Gromacs output files.
    tpr : str
        File name (not path) of run-input file.
    cpt : str
        File name (not path) of checkpoint file; need not exist.

    Returns
    -------
    firework 
        MD firework; can be submitted as part of a workflow to LaunchPad of
        choice.

    """
    tpr = os.path.join(launchdir, tpr)
    cpt = os.path.join(launchdir, cpt)

    if os.path.exists(cpt):
        files = [tpr, cpt]
    else:
        files = [tpr]

    # make scratch dir
    ft_makedir = PyTask(func='os.makedirs', args=[scratchdir])

    # rsync needed files
    if srcuser and srchost:
        ft_copy = FileRsyncTask(mode='remotesrc',
                                files=files,
                                dest=scratchdir,
                                srcuser=srcuser,
                                srchost=srchost)
    else:
        ft_copy = FileRsyncTask(mode='local',
                                files=files,
                                dest=scratchdir)

    # next, run MD
    ft_md = ScriptTask(script='run_md.sh', stdin_key=scratchdir, fizzle_bad_rc=True)
 
    # finally, copy back what's left
    if srcuser and srchost:
        ft_copyback = FileRsyncTask(mode='remotedest',
                                    files='{}/*'.format(scratchdir),
                                    dest=launchdir,
                                    destuser=srcuser,
                                    desthost=srchost)
    else:
        ft_copyback = FileRsyncTask(mode='local',
                                    files='{}/*'.format(scratchdir),
                                    dest=launchdir)
    
    # periodically pull the latest files in the background
    bg1 = BackgroundTask(ft_copyback, sleep_time=3600, run_on_finish=True)
    
    fw_md = Firework([ft_makedir, ft_copy, ft_md], 
                     spec={'_launch_dir': launchdir,
                           '_category': 'md',
                           '_background_tasks': [bg1],
                           '_queueadapter': {'launch_dir': scratchdir}},
                     name=name)

    return fw_md
