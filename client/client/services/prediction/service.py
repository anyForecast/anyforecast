from ..base import BaseService


class PredictionService(BaseService):
    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def post(self, training_task_id, dataset_group_name, dataset_name,
             prediction_params, pivot):
        api_params = {
            'predictor': {
                'task_id': training_task_id
            },
            'dataset': {
                'dataset_group_name': dataset_group_name,
                'dataset_name': dataset_name
            },
            'pivot': {
                'pivot': pivot
            },
            'prediction_params': prediction_params
        }
        return self._make_api_call(api_params)
