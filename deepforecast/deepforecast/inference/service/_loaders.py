import json
import os
from collections import OrderedDict

from . import API_ROOT


class JSONFileLoader:
    """Loader JSON files.
    """

    BUILTIN_DATA_PATH = os.path.join(API_ROOT, 'data')

    def __init__(
            self, extra_search_paths=None, include_default_search_paths=True
    ):

        if extra_search_paths is not None:
            self._search_paths = extra_search_paths
        else:
            self._search_paths = []

        if include_default_search_paths:
            self._search_paths.extend([self.BUILTIN_DATA_PATH])

    @property
    def search_paths(self):
        return self._search_paths

    def load_data(self, name):
        """Load data given a data path.

        This is a low level method that will search through the various
        search paths until it's able to load a value.

        Parameters
        ----------
        name : str
            The data path, i.e ``ec2/2015-03-01/service-2``.

        Raises
        ------
        DataNotFoundError if no data could be found.

        Returns
        -------
        data : dict
            The loaded data.
        """
        for possible_path in self._potential_locations(name):
            found = self.load_file(possible_path)
            if found is not None:
                return found
        # We didn't find anything that matched on any path.
        raise DataNotFoundError(data_path=name)

    def load_file(self, file_path):
        """Attempt to load the file path.

        Parameters
        ----------

        file_path: str
            The full path to the file to load without the '.json' extension.

        Returns
        -------
        file: OrderedDict
            The loaded data if it exists, otherwise None.
        """
        full_path = file_path + '.json'
        if not os.path.isfile(full_path):
            return

        # By default, the file will be opened with locale encoding on Python 3.
        # We specify "utf8" here to ensure the correct behavior.
        with open(full_path, 'rb') as fp:
            payload = fp.read().decode('utf-8')

        return json.loads(payload, object_pairs_hook=OrderedDict)

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
