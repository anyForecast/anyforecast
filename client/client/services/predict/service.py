from ..base import BaseService


class PredictService(BaseService):
    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def post(self, model_name, dataset_group_name, dataset_name, start, end,
             group_params, pivot, return_truth):
        api_params = {
            'predictor': {
                'model_name': model_name
            },
            'dataset': {
                'dataset_group_name': dataset_group_name,
                'dataset_name': dataset_name
            },
            'date_range': {
                'start': start,
                'end': end
            },
            'pivot': {
                'bool': pivot
            },
            'return_truth': {
                'bool': return_truth
            },
            'group_params': group_params
        }
        return self._make_api_call(api_params)
