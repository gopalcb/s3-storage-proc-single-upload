import os
import traceback

def set_environment_variables(**kwargs):
    """
    **kwargs = {'KEY1': 'value', 'KEY2': 'value', ...}
    ...
    """
    for arg in kwargs.values():
        for key, val in arg.items():
            os.environ[key] = val


def get_environment_variables(keys):
    """
    keys = ['KEY1', 'KEY2', ...]
    """
    try:
        results = []
        for key in keys:
            val = os.getenv(key)
            results.append(val)
    
        return results
        
    except Exception as e:
        print(f'ERROR: {traceback.format_exc()}')
        return []