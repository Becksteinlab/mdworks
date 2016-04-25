
from fireworks import ScriptTask
from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from fireworks.core.rocket_launcher import rapidfire

from fireworks.features.background_task import BackgroundTask

def submit_md(launchdir, launchpad='default', deffnm='md'):
    """Submit an MD workflow.

    """
    if launchpad == 'default':
        lpad = fireworks.LaunchPad.auto_load()
    elif isinstance(launchpad, :
        lpad 
    
    ft1 = fireworks.ScriptTask(script='/nfs/homes4/dldotson/.fireworks/run_md.sh')
    
    # periodically pull the latest files in the background
    bg1 = BackgroundTask(fireworks.ScriptTask(script=sim['WORK/pull.sh'].abspath),
                                              sleep_time=3600)
    
    fw_md = fireworks.Firework(ft1, spec={'_launch_dir': sim['WORK/'].abspath,
                                          '_category': 'workstationq',
                                          '_background_tasks': [bg1],
                                          '_queueadapter': {'launch_dir': sim['WORK/'].abspath}},
                               name="{}_MD".format(sim.name))
    
    lpad.add_wf(fw_md)
