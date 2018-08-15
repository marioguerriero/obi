from profiles import PROFILES

_profile_instances = [obj() for obj in PROFILES]

get_profile = dict([(p.name, p) for p in _profile_instances])
