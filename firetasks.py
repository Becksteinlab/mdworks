
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

