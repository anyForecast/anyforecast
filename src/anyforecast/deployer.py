import abc

from anyforecast import predictor


class Deployer(abc.ABC):

    def deploy(self, model_uri: str) -> predictor.Predictor:
        endpoint = self.endpoint(model_uri)
        return predictor.Predictor(endpoint)

    @abc.abstractmethod
    def endpoint(self, model_uri: str) -> str:
        """Deploys MLflow hosted model.

        Parameters
        ----------
        model_uri: str
            URI of model to deploy.

        Returns
        -------
        endpoint : str
            Deployment endpoint.
        """
        pass
