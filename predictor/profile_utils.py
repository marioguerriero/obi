from profiles import PROFILES


def infer_profile(req):
    """
    This function attempts to decide which is the most suitable profile
    to fulfil the user's request.
    :param req:
    :return:
    """
    _profile_instances = [obj() for obj in PROFILES]

    # get_profile = dict([(p.name, p) for p in _profile_instances])

    # TODO

    return None
