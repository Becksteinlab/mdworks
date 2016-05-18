from __future__ import unicode_literals

import os

from fireworks import FireTaskBase, FWAction
import gromacs


class GromacsContinueTask(FireTaskBase):
    """
    A FireTask to check the step listed in the CPT file against the total
    number of steps desired in the TPR file. If there are steps left to
    go, another MD workflow is submitted.

    Requires `gmx dump` be present in the session's PATH.

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
    postrun_wf : Workflow
        Workflow to perform after each copyback; performed in parallel to continuation run.
    post_wf : Workflow
        Workflow to perform after completed MD (no continuation); use for final
        postprocessing. 
    files : list 
        Names of files (not paths) needed for each leg of the simulation. Need
        not exist, but if they do they will get staged before each run.

    """
    _fw_name = 'GromacsContinueTask'
    required_params = ["sim",
                       "archive",
                       "stages",
                       "md_engine",
                       "local_category",
                       "md_category",
                       "postrun_wf",
                       "post_wf",
                       "files"]

    def run_task(self, fw_spec):
        from ..interactive import make_md_workflow

        # bit of an ad-hoc way to grab the checkpoint file
        cpt = [f for f in self['files']
                   if (('cpt' in f) and ('prev' not in f))]

        if len(cpt) > 1:
            raise ValueError("Multiple CPT files in 'files'; include "
                             "only one.")
        elif len(tpr) < 1:
            raise ValueError("No CPT file in 'files'; "
                             "cannot do continue check.")
        else:
            cpt = os.path.join(self['archive'], cpt[0])

        # bit of an ad-hoc way to grab the tpr file
        tpr = [f for f in self['files'] if ('tpr' in f)]

        if len(tpr) > 1:
            raise ValueError("Multiple TPR files in 'files'; include "
                             "only one.")
        elif len(tpr) < 1:
            raise ValueError("No TPR file in 'files'; "
                             "cannot do continue check.")
        else:
            tpr = os.path.join(self['archive'], tpr[0])

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
                                  md_engine=self['md_engine'],
                                  md_category=self['md_category'],
                                  local_category=self['local_category'],
                                  postrun_wf=self['postrun_wf'],
                                  files=self['files'])

            return FWAction(additions=[wf])

        elif self.get('post_wf'):
            # otherwise, we submit the post workflow
            if isinstance(post_wf, dict):
                post_wf = Workflow.from_dict(post_wf)

            # this makes a fresh copy without already-used fw_ids
            post_wf = Workflow.from_wflow(post_wf)

            return FWAction(additions=[post_wf])


