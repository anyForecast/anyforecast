from client.services.base import BaseService


class Forecast(BaseService):

    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def create_predictor(self, name, algorithm, forecast_horizon,
                         dataset_name, perform_hpo=False):
        api_params = self._create_api_params(
            name=name,
            algorithm=algorithm,
            forecast_horizon=forecast_horizon,
            dataset_name=dataset_name,
            perform_hpo=perform_hpo
        )
        return self._make_api_call(api_params)
