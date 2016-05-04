import os

import mdsynthesis as mds

from fireworks import ScriptTask, PyTask, FileTransferTask
from fireworks import Workflow
from fireworks.core.firework import Firework

from mdworks.firetasks import FilePullTask, BeaconTask, ContinueTask

def make_md_workflow(sim, archive, stages, modulesrc=None, gmxmodule=None,
                     md_category='md', postprocessing_wf=None, deffnm='md',
                     tpr='md.tpr', cpt='md.cpt'):
    """Construct an MD workflow.

    Parameters
    ----------
    sim : str
        MDSynthesis Sim.
    archive : str
        Absolute path to directory to launch from, which holds all required data.
    stages : list
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

    Returns
    -------
    firework 
        MD workflow; can be submitted to LaunchPad of choice.

    """
    sim = mds.Sim(sim)

    if os.path.exists(os.path.join(archive, cpt)):
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
                                          retry=True,
                                          shell_interpret=True))

    fw_stage = Firework(fts_stage,
                        spec={'_launch_dir': archive,
                              '_category': 'local'},
                        name='staging')


    ## MD execution; takes place in queue context of compute resource

    # copy input files to scratch space
    ft_copy = FileTransferTask(mode='copy',
                               files=[os.path.join('${STAGING}/', sim.uuid, i) for i in files],
                               dest='${SCRATCHDIR}/',
                               shell_interpret=True)

    # next, run MD
    ft_md = ScriptTask(script='run_md.sh', fizzle_bad_rc=True)

    # send info on where files live to pull firework
    ft_info = BeaconTask()

    fw_md = Firework([ft_copy, ft_md, ft_info],
                     spec={'_category': md_category},
                     name='md',
                     parents=fw_stage)

 
    ## Pull files back to archive; takes place locally
    ft_copyback = FilePullTask(dest=archive)

    fw_copyback = Firework([ft_copyback],
                           spec={'_launch_dir': archive,
                                 '_category': 'local'},
                           name='pull',
                           parents=fw_md)


    ## Decide if we need to continue and submit new workflow if so; takes place
    ## locally
    ft_continue = ContinueTask(sim=sim,
                               archive=archive,
                               stages=stages,
                               md_category=md_category,
                               postprocessing_wf=postprocessing_wf,
                               tpr=tpr,
                               cpt=cpt)

    fw_continue = Firework([ft_continue],
                           spec={'_launch_dir': archive,
                                 '_category': 'local'},
                           name='continue',
                           parents=fw_copyback)

    wf = Workflow([fw_stage, fw_md, fw_copyback, fw_continue],
                  name='{} | md'.format(sim.name),
                  metadata=dict(sim.categories))

    ## Mix in postprocessing workflow, if given
    if postprocessing_wf:
        if isinstance(postprocessing_wf, dict):
            postprocessing_wf = Workflow.from_dict(postprocessing_wf)

        wf.append_wf(postprocessing_wf, [fw_copyback.fw_id])
        
    return wf
