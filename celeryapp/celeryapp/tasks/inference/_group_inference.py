from .service import InferenceSession
from .._base_task import BaseTask
from .._transformers import InferenceTransformer


class BaseInference(BaseTask):
    def __init__(self):
        super().__init__()

    def make_session(self):
        return InferenceSession()

    def make_inference(self, X, service_name, model_name):
        session = self.make_session()
        client = session.create_client(service_name, model_name)
        return client.make_inference(X)

    def make_inference_transformer(
            self, group_ids, timestamp, date_range, what_ifs
    ):
        return InferenceTransformer(
            group_ids=group_ids, timestamp_col=timestamp,
            date_range=date_range, what_if_data=what_ifs
        )

    def run(self, model, data, date_range, what_ifs):
        X = data['DataFrame']
        feature_schema = data['FeaturesSchema']

        inference_transformer = self.make_inference_transformer(
            group_ids=feature_schema.get_names('group_ids'),
            timestamp=feature_schema.get_names('timestamp'),
            date_range=date_range,
            what_ifs=what_ifs
        )

        X = inference_transformer.fit_transform(X)
        return self.make_inference(X, **model)
