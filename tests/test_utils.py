import tests.test_data as test_data

from netsim.utils import load_resource, yaml_to_dict


def test_load_resource1():
    load_resource("2tierClos.yaml", test_data)


def test_load_yaml1():
    yaml_to_dict(load_resource("2tierClos.yaml", test_data))
