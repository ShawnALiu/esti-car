import os

_data_path = os.path.join(os.path.expanduser("~"), "EstiCar")


def get_data_path():
    return _data_path


def set_data_path(path):
    global _data_path
    _data_path = path