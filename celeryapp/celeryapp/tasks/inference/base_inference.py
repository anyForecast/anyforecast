from typing import Tuple, Dict, List

import pandas as pd
from skorch_forecasting.utils import datetime

from .service.session import InferenceSession
from .. import base, preprocessing, serializers, schemas



class BaseInference(base.BaseTask):
    def __init__(self):
        super().__init__()
        self.session = InferenceSession()
        self._serializer = serializers.PandasSerializer(
            orient='split', outer_key='dataframe_split')

    def make_inference_transformer(
            self, group_ids, timestamp, date_range, what_ifs
    ):
        return preprocessing.InferenceTransformer(
            group_ids=group_ids, timestamp_col=timestamp, date_range=date_range,
            what_if_data=what_ifs)

    def create_client(self, model_name, service_name):
        return self.session.create_client(
            model_name, service_name, is_secure=False,
            serializer=self.get_serializer())

    def make_inference(self, X, client):
        return client.make_inference(X)

    def get_serializer(self):
        return self._serializer

    def run(
            self,
            data_and_schema: Tuple[pd.DataFrame, schemas.FeaturesSchema],
            model: Dict,
            date_range: Dict,
            what_ifs: List[Dict]
    ):
        X, schema = data_and_schema

        model_client = self.create_client(**model)
        model_params = model_client.model.get_params()

        # Create date range.
        date_range = DateRangeCreator(
            **date_range, freq=model_params['freq'],
            max_encoder_length=model_params['max_encoder_length']
        ).create_date_range()

        # Make inference transformer.
        inference_transformer = self.make_inference_transformer(
            group_ids=schema.group_ids, timestamp=schema.timestamp,
            date_range=date_range, what_ifs=what_ifs)
        X = inference_transformer.fit_transform(X)

        # Object of type Timestamp is not JSON serializable.
        X[schema.timestamp] = X[schema.timestamp].astype(str)

        return self.make_inference(X, model_client)


class DateRangeCreator:
    def __init__(self, start_date, end_date, freq, max_encoder_length):
        self.start_date = start_date
        self.end_date = end_date
        self.freq = freq
        self.max_encoder_length = max_encoder_length

    def create_date_range(self):
        start, end = map(self.map_to_freq, (self.start_date, self.end_date))
        encoder_dates = self.make_encoder_dates(start)
        decoder_dates = self.make_decoder_dates(start, end)
        return encoder_dates.union(decoder_dates)

    def make_encoder_dates(self, start):
        periods = self.max_encoder_length + 1
        return pd.date_range(
            end=start, periods=periods, inclusive='left', freq=self.freq)

    def make_decoder_dates(self, start, end):
        return pd.date_range(start, end, freq=self.freq)

    def map_to_freq(self, date):
        return datetime.set_date_on_freq(date, self.freq)
