from abc import ABC, abstractmethod


class Deployer(ABC):
    @abstractmethod
    def deploy(self, name: str, model_uri: str) -> None:
        """Deploy interace.

        Parameters
        ----------
        name: str
            Unique name to use for deployment. Also used as model image when
            built.

        model_uri: str
            URI of model to deploy.
        """
        pass
