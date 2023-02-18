from abc import abstractmethod, ABCMeta

from .... import dataloaders


class ModelCreator:
    def __init__(self, service_name):
        self.service_name = service_name

    def create_model(self, model_name):
        models = {
            'mlflow': MLFlowModel
        }

        return models[self.service_name](model_name)


class Model(metaclass=ABCMeta):

    def load_model(self):
        pass

    @abstractmethod
    def get_params(self):
        pass

    def get_metrics(self):
        pass


class MLFlowModel:
    def __init__(self, model_name):
        self.model_name = model_name
        self._loader = dataloaders.MlFlowLoader()

    def load_model(self, stage='production'):
        return self._loader.load_predictor(self.model_name, stage)

    def get_params(self):
        mv = self._loader.get_latest_model_version(self.model_name)
        return self._loader.get_run_id_params(mv.run_id)

    def get_metrics(self):
        pass