from six import string_types
import os

import mdsynthesis as mds
import yaml

from fireworks import ScriptTask, PyTask, FileTransferTask, StagingTask
from fireworks import Workflow, Firework

from .firetasks import FilePullTask, BeaconTask, MkRunDirTask
from .gromacs.firetasks import GromacsContinueTask


def make_md_workflow(sim, archive, stages, files, md_engine='gromacs',
                     md_category='md', local_category='local',
                     postrun_wf=None, post_wf=None):
    """Construct a general, single MD simulation workflow.

    Assumptions
    -----------
    Queue launcher submission script must define and export the following
    environment variables:

        1. STAGING : absolute path on resource to staging directory
        2. SCRATCH : absolute path on resource to scratch directory

    The staging directory must already exist on all resources specified in
    ``stages``.

    The script ``run_md.sh`` must be somewhere on your path, and must take
    a single argument giving the directory to execute MD out of. It should
    create and change the working directory to that directory before anything
    else.

    Parameters
    ----------
    sim : str
        MDSynthesis Sim.
    archive : str
        Absolute path to directory to launch from, which holds all required
        files for running MD. 
    stages : list, str
        Dicts giving for each of the following keys:
            - 'server': server host to transfer to
            - 'user': username to authenticate with
            - 'staging': absolute path to staging area on remote resource
        alternatively, a path to a yaml file giving a list of dictionaries
        with the same information.
    files : list 
        Names of files (not paths) needed for each leg of the simulation. Need
        not exist, but if they do they will get staged before each run.
    md_engine : {'gromacs'}
        MD engine name; needed to determine continuation mechanism to use.
    md_category : str
        Category to use for the MD Firework. Used to target to correct rockets.
    local_category : str
        Category to use for non-MD Fireworks, which should be run by rockets
        where the ``archive`` directory is accessible.
    postrun_wf : Workflow
        Workflow to perform after each copyback; performed in parallel to continuation run.
    post_wf : Workflow
        Workflow to perform after completed MD (no continuation); use for final
        postprocessing. 

    Returns
    -------
    workflow 
        MD workflow; can be submitted to LaunchPad of choice.

    """
    sim = mds.Sim(sim)

    #TODO: perhaps move to its own FireTask?
    sim.categories['md_status'] = 'running'

    ft_stage = StagingTask(stages=stages,
                           files=files,
                           archive=archive,
                           uuid=sim.uuid,
                           shell_interpret=True,
                           max_retry=5,
                           allow_missing=True)

    fw_stage = Firework([ft_stage],
                        spec={'_launch_dir': archive,
                              '_category': local_category},
                        name='staging')

    ## MD execution; takes place in queue context of compute resource

    # make rundir
    ft_mkdir = MkRunDirTask(uuid=sim.uuid)

    # copy input files to scratch space
    ft_copy = FileTransferTask(mode='copy',
                               files=[os.path.join('${STAGING}/', sim.uuid, i) for i in files],
                               dest=os.path.join('${SCRATCHDIR}/', sim.uuid),
                               ignore_missing=True,
                               shell_interpret=True)

    # next, run MD
    ft_md = ScriptTask(script='run_md.sh {}'.format(
                            os.path.join('${SCRATCHDIR}/', sim.uuid)),
                       use_shell=True,
                       fizzle_bad_rc=True)

    # send info on where files live to pull firework
    ft_info = BeaconTask(uuid=sim.uuid)

    fw_md = Firework([ft_mkdir, ft_copy, ft_md, ft_info],
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
        ft_continue = GromacsContinueTask(sim=sim, archive=archive,
                stages=stages, files=files, md_engine=md_engine,
                md_category=md_category, local_category=local_category,
                postrun_wf=postrun_wf, post_wf=post_wf)
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

    ## Mix in postrun workflow, if given
    if postrun_wf:
        if isinstance(postrun_wf, dict):
            postrun_wf = Workflow.from_dict(postrun_wf)

        wf.append_wf(Workflow.from_wflow(postrun_wf), [fw_copyback.fw_id])
        
    return wf
