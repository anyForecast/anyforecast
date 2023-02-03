from .service.session import InferenceSession
from .._base_task import BaseTask
from .._transformers import InferenceTransformer
from ..serializers import PandasSerializer


class BaseInference(BaseTask):
    def __init__(self):
        super().__init__()
        self._serializer = PandasSerializer(
            orient='split', outer_key='dataframe_split')

    def make_session(self):
        return InferenceSession()

    def make_inference_transformer(
            self, group_ids, timestamp, date_range, what_ifs
    ):
        return InferenceTransformer(
            group_ids=group_ids, timestamp_col=timestamp, date_range=date_range,
            what_if_data=what_ifs)

    def make_inference(self, X, model):
        session = self.make_session()
        client = session.create_client(
            **model, is_secure=False, serializer=self.get_serializer())
        return client.make_inference(X)

    def get_serializer(self):
        return self._serializer

    def run(self, model, data, date_range, what_ifs):
        X = data['DataFrame']
        feature_schema = data['FeaturesSchema']

        # Make inference transformer.
        group_ids = feature_schema.get_names('group_ids')
        timestamp = feature_schema.get_names('timestamp')
        inference_transformer = self.make_inference_transformer(
            group_ids=group_ids, timestamp=timestamp, date_range=date_range,
            what_ifs=what_ifs)
        X = inference_transformer.fit_transform(X)

        # Object of type Timestamp is not JSON serializable.
        X[timestamp] = X[timestamp].astype(str)

        return self.make_inference(X, model)
