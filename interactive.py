import os

from fireworks import ScriptTask, PyTask, FileTransferTask
from fireworks.core.firework import Firework

from fireworks.features.background_task import BackgroundTask

from mdworks.firetasks import FilePull

def make_md_workflow(sim, archive, stages, deffnm='md', tpr='md.tpr', cpt='md.cpt'):
    """Construct an MD workflow.

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
    tpr : str
        File name (not path) of run-input file.
    cpt : str
        File name (not path) of checkpoint file; need not exist.

    Returns
    -------
    firework 
        MD workflow; can be submitted to LaunchPad of choice.

    """
    if os.path.exists(cpt):
        files = [tpr, cpt]
    else:
        files = [tpr]

    ## Stage files on all resources where MD may run; takes place locally
    fts_stage = list()
    for stage in stages:
        fts_stage.append(FileTransferTask(mode='rtransfer',
                                          server=stage['server'],
                                          user=stage['user'],
                                          files=[os.path.join(archive, i) for i in files],
                                          dest=os.path.join(stage['staging'], sim.uuid),
                                          shell_interpret=True))

    fw_stage = Firework(fts_stage,
                        spec={'_launch_dir': archive,
                              '_category': 'local'},
                        name='staging')


    ## MD execution; takes place in queue context of compute resource

    # copy input files to scratch space
    ft_copy = FileTransferTask(mode='copy',
                               files=[os.path.join('$STAGING/', sim.uuid, i) for i in files],
                               dest='$WORK/',
                               shell_interpret=True)

    # next, run MD
    ft_md = ScriptTask(script='run_md.sh', stdin_key, fizzle_bad_rc=True)

    # send info on where files live to pull firework
    ft_info = BeaconTask()

    fw_md = Firework([ft_copy, ft_md],
                     spec={'_category': 'md',
                           '_pass_job_info': True},
                     name='md',
                     parents=fw_stage)

 
    ## Pull files back to archive; takes place locally
    ft_copyback = FilePull(files=[],                        # updated by previous FW
                           dest=archive,
                           server=None,                     # updated by previous FW
                           spec={'_launch_dir': archive,
                                 '_category': 'local'},
                           name='pull',
                           parents=fw_md)


    ft_copyback = FileRsyncTask(mode='remotedest',
                                    files='{}/*'.format(scratchdir),
                                    dest=launchdir,
                                    destuser=srcuser,
                                    desthost=srchost)
    
    return wf
