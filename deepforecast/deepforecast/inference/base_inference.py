from typing import Tuple, Dict, List

import pandas as pd
from skorch_forecasting.utils import datetime

from .service.session import InferenceSession
from anyforecast.anyforecast.celeryapp.tasks import schemas
from .. import serializers, preprocess, base



class DatetimeLocator(BaseEstimator):
    """Locates DataFrame rows by date range.

    If the passed ``date_range`` is already contained X,
    the rows inside it are returned. That is, in such case,
    this transformation is equivalent to a simple datetime filter.

    Parameters
    ----------
    timestamp_col : str
        Datetime column

    date_range : pd.DatetimeIndex
        Instance of :class:`pd.DatetimeIndex` obtained from
        :meth:`pd.date_range()`.
    """

    def __init__(
            self,
            timestamp,
            date_range
    ):
        self.timestamp = timestamp
        self.date_range = date_range

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return self._transform(X)

    def _transform(self, X):
        Xt = X.set_index(self.timestamp_col).reindex(self.date_range)
        return Xt.fillna(method='ffill').fillna(method='bfill')


class GroupwiseDatetimeLocator(BaseEstimator):
    def __init__(
            self,
            timestamp,
            date_range,
            group_ids=None,
            reset_index=True
    ):
        self.timestamp = timestamp
        self.date_range = date_range
        self.group_ids = group_ids
        self.reset_index = reset_index

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        dt_locator = DatetimeLocator(self.timestamp, self.date_range)

        X = X.groupby(self.group_ids, sort=False) \
            .apply(dt_locator.transform) \
            .drop(self.group_ids, axis='columns')

        if self.reset_index:
            return X.reset_index().rename(
                columns={'level_1': self.timestamp_col})
        return X


class WhatIfTransformer(BaseEstimator):

    def __init__(
            self,
            timestamp,
            date_range,
            input_cols,
            value=None,
            method=None,
            percentage=None
    ):
        self.timestamp = timestamp
        self.date_range = date_range
        self.input_cols = input_cols
        self.value = value
        self.method = method
        self.percentage = percentage

    def transform(self, X):
        mask = self._make_bool_mask(X)

        if self.percentage is not None:
            return self._percentage_change(X, mask)

        X = self._place_nans(X, mask)
        return X.fillna(value=self.value, method=self.method)

    def _make_bool_mask(self, X):
        return X[self.timestamp_col].isin(self.date_range)

    def _place_nans(self, X, mask):
        X.loc[mask, self.input_cols] = np.nan
        return X

    def _percentage_change(self, X, mask):
        factor = 1 + (self.percentage / 100)

        X.loc[mask, self.input_cols] *= factor
        return X


class InferenceTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, group_ids, timestamp_col, date_range, what_if_data=None):
        self.group_ids = group_ids
        self.timestamp_col = timestamp_col
        self.date_range = date_range
        self.what_if_data = what_if_data

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        pipeline = self._make_pipeline()
        return pipeline.fit_transform(X)

    def _make_pipeline(self):
        steps = [self._make_datetime_locator()]

        if self.what_if_data is not None:
            steps.append(self._make_what_if_transformer())

        return make_pipeline(*steps)

    def _make_what_if_transformer(self):
        return BulkWhatIfTransformer(
            self.group_ids, self.timestamp_col, self.date_range,
            self.what_if_data)

    def _make_datetime_locator(self):
        return GroupWiseDatetimeLocator(
            self.timestamp_col, self.date_range, group_ids=self.group_ids)



class BaseInference(base.BaseTask):
    def __init__(self):
        super().__init__()
        self.session = InferenceSession()
        self._serializer = serializers.PandasSerializer(
            orient='split', outer_key='dataframe_split')

    def make_inference_transformer(
            self, group_ids, timestamp, date_range, what_ifs
    ):
        return preprocess.InferenceTransformer(
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
