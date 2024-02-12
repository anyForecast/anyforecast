import abc

from anyforecast import predictor


class Deployer(abc.ABC):

    def deploy(self, model_uri: str) -> predictor.Predictor:
        endpoint = self._deploy(model_uri)
        return predictor.Predictor(endpoint)

    @abc.abstractmethod
    def _deploy(self, model_uri: str) -> str:
        """Deploy interace.

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
