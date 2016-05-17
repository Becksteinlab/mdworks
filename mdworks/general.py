import os

import mdsynthesis as mds

from fireworks import ScriptTask, PyTask, FileTransferTask
from fireworks import Workflow, Firework

from .firetasks import FilePullTask, BeaconTask
from .gromacs.firetasks import GromacsContinueTask


def make_md_workflow(sim, archive, stages, md_engine='gromacs',
                     md_category='md', local_category='local',
                     postprocessing_wf=None, files=None):
    """Construct a general, single MD simulation workflow.

    Assumptions
    -----------
    Queue launcher submission script must define and export the following
    environment variables:

        1. STAGING : absolute path on resource to staging directory
        2. SCRATCH : absolute path on resource to scratch directory

    The staging directory must already exist on all resources specified in
    ``stages``.

    Parameters
    ----------
    sim : str
        MDSynthesis Sim.
    archive : str
        Absolute path to directory to launch from, which holds all required
        files for running MD. 
    stages : list
        Dicts giving for each of the following keys:
            - 'server': server host to transfer to
            - 'user': username to authenticate with
            - 'staging': absolute path to staging area on remote resource
    md_engine : {'gromacs'}
        MD engine name; needed to determine continuation mechanism to use.
    md_category : str
        Category to use for the MD Firework. Used to target to correct rockets.
    local_category : str
        Category to use for non-MD Fireworks, which should be run by rockets
        where the ``archive`` directory is accessible.
    postprocessing_wf : Workflow
        Workflow to perform after copyback; performed in parallel to continuation run.
    files : list 
        Names of files (not paths) needed for each leg of the simulation. Need
        not exist, but if they do they will get staged before each run.

    Returns
    -------
    workflow 
        MD workflow; can be submitted to LaunchPad of choice.

    """
    sim = mds.Sim(sim)

    #TODO: perhaps move to its own FireTask?
    sim.categories['md_status'] = 'running'

    #TODO: the trouble with this is that if this workflow is created with the intent
    #      of being attached to another, these files may not exist at all yet
    f_exist = [f for f in files if os.path.exists(os.path.join(archive, f))]

    ## Stage files on all resources where MD may run; takes place locally
    fts_stage = list()
    for stage in stages:
        fts_stage.append(FileTransferTask(mode='rtransfer',
                                          server=stage['server'],
                                          user=stage['user'],
                                          files=[os.path.join(archive, i) for i in f_exist],
                                          dest=os.path.join(stage['staging'], sim.uuid),
                                          retry=True,
                                          shell_interpret=True))

    fw_stage = Firework(fts_stage,
                        spec={'_launch_dir': archive,
                              '_category': local_category},
                        name='staging')


    ## MD execution; takes place in queue context of compute resource

    # copy input files to scratch space
    ft_copy = FileTransferTask(mode='copy',
                               files=[os.path.join('${STAGING}/', sim.uuid, i) for i in f_exist],
                               dest='${SCRATCHDIR}/',
                               shell_interpret=True)

    # next, run MD
    ft_md = ScriptTask(script='run_md.sh', fizzle_bad_rc=True)

    # send info on where files live to pull firework
    ft_info = BeaconTask()

    fw_md = Firework([ft_copy, ft_md, ft_info],
                     spec={'_category': md_category},
                     name='md')

 
    ## Pull files back to archive; takes place locally
    ft_copyback = FilePullTask(dest=archive)

    fw_copyback = Firework([ft_copyback],
                           spec={'_launch_dir': archive,
                                 '_category': local_category},
                           name='pull')


    ## Decide if we need to continue and submit new workflow if so; takes place
    ## locally

    if md_engine == 'gromacs':
        ft_continue = GromacsContinueTask(
                sim=sim, archive=archive, stages=stages, md_engine=md_engine,
                md_category=md_category, local_category=local_category,
                postprocessing_wf=postprocessing_wf, files=files)
    else:
        raise ValueError("No known md engine `{}`.".format(md_engine))

    fw_continue = Firework([ft_continue],
                           spec={'_launch_dir': archive,
                                 '_category': local_category},
                           name='continue')

    wf = Workflow([fw_stage, fw_md, fw_copyback, fw_continue],
                  links_dict={fw_stage: [fw_md], fw_md: [fw_copyback], fw_copyback: [fw_continue]},
                  name='{} | md'.format(sim.name),
                  metadata=dict(sim.categories))

    ## Mix in postprocessing workflow, if given
    if postprocessing_wf:
        if isinstance(postprocessing_wf, dict):
            postprocessing_wf = Workflow.from_dict(postprocessing_wf)

        wf.append_wf(Workflow.from_Workflow(postprocessing_wf), [fw_copyback.fw_id])
        
    return wf
