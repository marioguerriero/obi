from .profiles import *

_profile_instances = [obj() for name, obj in globals().items()
                      if not name.startswith('__') and name != 'Profile']

get_profile = dict([(p.name, p) for p in _profile_instances])
