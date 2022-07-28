from client.services.base import BaseService


class ForecastService(BaseService):

    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def post(self, task_name, dataset_group_name, dataset_name,
             algorithm, forecast_horizon, freq, perform_hpo=False):
        api_params = {
            'forecaster': {
                'task_name': task_name,
                'algorithm': algorithm,
                'forecast_horizon': forecast_horizon,
                'freq': freq,
                'perform_hpo': perform_hpo
            },
            'dataset': {
                'dataset_group_name': dataset_group_name,
                'dataset_name': dataset_name
            }
        }

        return self._make_api_call(api_params)
