from ._what_if import WhatIf
from ..base import BaseService


class PredictionService(BaseService):
    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def post(self, training_task_id, dataset_group_name, dataset_name,
             what_ifs=None):
        self._validate_what_ifs(what_ifs)
        api_params = {
            'estimator': {
                'task_id': training_task_id
            },
            'dataset': {
                'dataset_group_name': dataset_group_name,
                'dataset_name': dataset_name
            }
        }
        return self._make_api_call(api_params)

    def _validate_what_ifs(self, what_ifs):
        if what_ifs is not None:
            for what_if in what_ifs:
                if not isinstance(what_if, WhatIf):
                    raise ValueError()
