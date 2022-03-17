import json
import logging
import os
from collections import OrderedDict

from . import API_ROOT
from .exceptions import DataNotFoundError

logger = logging.getLogger(__name__)


def create_loader(search_path_string=None):
    """Create a Loader class.

    This factory function creates a loader given a search string path.

    Parameters
    ----------
    search_path_string : str, default=None
        The AWS_DATA_PATH value.  A string of data path values separated by
        the ``os.path.pathsep`` value, which is typically ``:`` on POSIX
        platforms and ``;`` on windows.

    Returns
    -------
    loader : Loader instance.
    """
    if search_path_string is None:
        return Loader()
    paths = []
    extra_paths = search_path_string.split(os.pathsep)
    for path in extra_paths:
        path = os.path.expanduser(os.path.expandvars(path))
        paths.append(path)
    return Loader(extra_search_paths=paths)


class JSONFileLoader:
    """Loader JSON files.
    This class can load the default format of models, which is a JSON file.
    """

    def exists(self, file_path):
        """Checks if the file exists.

        :type file_path: str
        :param file_path: The full path to the file to load without
            the '.json' extension.
        :return: True if file path exists, False otherwise.
        """
        return os.path.isfile(file_path + '.json')

    def load_file(self, file_path):
        """Attempt to load the file path.

        :type file_path: str
        :param file_path: The full path to the file to load without
            the '.json' extension.
        :return: The loaded data if it exists, otherwise None.
        """
        full_path = file_path + '.json'
        if not os.path.isfile(full_path):
            return

        # By default the file will be opened with locale encoding on Python 3.
        # We specify "utf8" here to ensure the correct behavior.
        with open(full_path, 'rb') as fp:
            payload = fp.read().decode('utf-8')

        logger.debug("Loading JSON file: %s", full_path)
        return json.loads(payload, object_pairs_hook=OrderedDict)


class Loader:
    """Find and load data models.

    This class will handle searching for and loading data models.
    The main method used here is ``load_service_model``, which is a
    convenience method over ``load_data`` and ``determine_latest_version``.
    """
    FILE_LOADER_CLASS = JSONFileLoader
    # The included models in botocore/data/ that we ship with botocore.
    BUILTIN_DATA_PATH = os.path.join(API_ROOT, 'data')
    # For convenience we automatically add ~/.aws/models to the data path.
    CUSTOMER_DATA_PATH = os.path.join(os.path.expanduser('~'),
                                      '.aws', 'models')

    def __init__(self, extra_search_paths=None, file_loader=None,
                 cache=None, include_default_search_paths=True):

        if cache is None:
            cache = {}
        self._cache = cache

        if file_loader is None:
            file_loader = self.FILE_LOADER_CLASS()
        self.file_loader = file_loader

        if extra_search_paths is not None:
            self._search_paths = extra_search_paths
        else:
            self._search_paths = []

        if include_default_search_paths:
            self._search_paths.extend([self.CUSTOMER_DATA_PATH,
                                       self.BUILTIN_DATA_PATH])

    @property
    def search_paths(self):
        return self._search_paths

    def load_data(self, name):
        """Load data given a data path.

        This is a low level method that will search through the various
        search paths until it's able to load a value.  This is typically
        only needed to load *non* model files (such as _endpoints and
        _retry).  If you need to load model files, you should prefer
        ``load_service_model``.

        :type name: str
        :param name: The data path, i.e ``ec2/2015-03-01/service-2``.
        :return: The loaded data.  If no data could be found then
            a DataNotFoundError is raised.
        """
        for possible_path in self._potential_locations(name):
            found = self.file_loader.load_file(possible_path)
            if found is not None:
                return found
        # We didn't find anything that matched on any path.
        raise DataNotFoundError(data_path=name)

    def _potential_locations(self, name=None, must_exist=False,
                             is_dir=False):
        # Will give an iterator over the full path of potential locations
        # according to the search path.
        for path in self.search_paths:
            if os.path.isdir(path):
                full_path = path
                if name is not None:
                    full_path = os.path.join(path, name)
                if not must_exist:
                    yield full_path
                else:
                    if is_dir and os.path.isdir(full_path):
                        yield full_path
                    elif os.path.exists(full_path):
                        yield full_path
