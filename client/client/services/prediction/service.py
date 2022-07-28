from ._what_if import WhatIf
from ..base import BaseService


class PredictionService(BaseService):
    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def post(self, dataset_group_name, dataset_name, what_ifs=None):
        self._validate_what_ifs(what_ifs)
        api_params = {
            'dataset': {
                'dataset_group_name': dataset_group_name,
                'dataset_name': dataset_name
            },
            'what_if': what_ifs
        }
        self._make_api_call(api_params)

    def _validate_what_ifs(self, what_ifs):
        for what_if in what_ifs:
            if not isinstance(what_if, WhatIf):
                raise ValueError()
