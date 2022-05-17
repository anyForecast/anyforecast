"""Utility functions for path operations.
"""

import os


def add_final_slash_to_path(uri):
    return uri if uri.endswith('/') else f'{uri}/'


def get_last_element_from_path(path):
    """Obtains the last element from `path`.

    Reference
    ---------
    https://stackoverflow.com/questions/3925096/how-to-get-only-the-last-part-of-a-path-in-python
    """
    return os.path.basename(os.path.normpath(path))


def list_subfolders(folder):
    return [f.path for f in os.scandir(folder) if f.is_dir()]
