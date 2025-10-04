import json
from uuid import uuid4

def read_json_config(path):
    """
    load json config file
    :param path:
    :return:
    """
    try:
        with open(path, 'r') as f:
            js = json.load(f)
        return js
    except Exception as e:
        return e


def write_json_config(data, path):
    """
    write dict based config
    :param data:
    :param path:
    :return:
    """
    try:
        with open(path, 'w') as f:
            json.dump(data, indent=4, fp=f)
        return True
    except Exception as e:
        return e

def get_unique_identifier():
    """
    get unique identifier
    :return:
    """
    return str(uuid4())

