from typing import Dict, Tuple, List, Optional

import pandas as pd
import skorch
from skorch_forecasting import nn

from . import _callbacks
from .. import base_trainer
from ... import schemas


def _category_to_object(pandas_df):
    """Private function that casts category columns to object.

    Parameters
    ----------
    pandas_df

    Returns
    -------
    pandas_df
    """
    dtypes = pandas_df.dtypes
    for name, dtype in dtypes.items():
        if dtype.name == 'category':
            pandas_df[name] = pandas_df[name].astype(object)
    return pandas_df


class SkorchForecastingArgsCreator:

    def __init__(
            self,
            group_ids: List[str],
            timestamp: str,
            target: str,
            time_varying_known: List[str],
            time_varying_unknown: List[str],
            static_categoricals: List[str]
    ):
        self.group_ids = group_ids
        self.timestamp = timestamp
        self.target = target
        self.time_varying_known = time_varying_known
        self.time_varying_unknown = time_varying_unknown
        self.static_categoricals = static_categoricals

    def create_args(
            self,
            max_prediction_length: int,
            hparams: Optional[Dict] = None
    ) -> Dict:
        if hparams is None:
            hparams = {}

        max_encoder_length = hparams.get(
            'max_encoder_length', max_prediction_length)

        time_varying_unknown = self.time_varying_unknown + [self.target]

        return {
            'group_ids': self.group_ids,
            'time_idx': self.timestamp,
            'target': self.target,
            'time_varying_known_reals': self.time_varying_known,
            'time_varying_unknown_reals': time_varying_unknown,
            'static_categoricals': self.static_categoricals,
            'max_prediction_length': max_prediction_length,
            'max_encoder_length': max_encoder_length,
            'cv_split': None,
            **hparams
        }


class SkorchForecastingCreator:
    """Creates skorch-forecasting neural net estimators.

    Parameters
    ----------
    group_ids : list of str
        List of column names identifying a time series. This means that the
        ``group_ids`` identify a sample together with ``date``. If you
        have only one times series, set this to the name of column that is
        constant.

    timestamp : str
        Time index column.

    target : str
        Target column.

    time_varying_known : list of str
        List of continuous variables that change over time and are known in the
        future (e.g. price of a product, but not demand of a product).

    time_varying_unknown : list of str
        List of continuous variables that change over time and are not known in
        the future. You might want to include your ``target`` here.

    static_categoricals : list of str
        List of categorical variables that do not change over time (also known
        as `time independent variables`). You might want to include your
        ``group_ids`` here for the learning algorithm to distinguish between
        different time series.

    """

    def __init__(self, group_ids, timestamp, target, time_varying_known,
                 time_varying_unknown, static_categoricals):
        self._args_creator = SkorchForecastingArgsCreator(
            group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals)

    def get_estimator_cls(self, algorithm):
        return getattr(nn, algorithm)

    def create_estimator(
            self,
            algorithm: str,
            max_prediction_length: int,
            hparams: Dict
    ) -> nn.TimeseriesNeuralNet:
        """Creates estimator.

        Parameters
        ----------
        algorithm : str
        max_prediction_length : int
        hparams: dict

        Returns
        -------
        skorch-forecasting estimator
        """
        args = self._args_creator.create_args(max_prediction_length, hparams)
        estimator_cls = self.get_estimator_cls(algorithm)
        return estimator_cls(**args)

    @classmethod
    def from_schema(
            cls,
            schema: schemas.FeaturesSchema
    ) -> "SkorchForecastingCreator":
        """From schema factory.

        Parameters
        ----------
        schema : FeaturesSchema

        Returns
        -------
        SkorchForecastingCreator
        """
        return cls(**vars(schema))


class SkorchForecastingTrainer(base_trainer.BaseTrainer):
    """Trains skorch forecasting estimator.
    """

    def __init__(self):
        super().__init__()

    def run(
            self,
            data_and_schema: Tuple[pd.DataFrame, schemas.FeaturesSchema],
            trainer: Dict
    ) -> None:
        """Runs training task.

        Parameters
        ----------
        data_and_schema : tuple
            Tuple of (pd.DataFrame, FeatureSchema)

        trainer : dict
            Training settings.
        """
        data, schema = data_and_schema

        # Currently, MLFlow does not support category data type when inferring
        # the signature (https://github.com/mlflow/mlflow/issues/3849).
        data = _category_to_object(data)

        # Create preprocessor.
        preprocessor = self.create_timeseries_preprocessor(
            group_ids=schema.group_ids, timestamp=schema.timestamp,
            target=schema.target, freq=trainer['freq'],
            preprocessing_data=trainer['preprocessing'])

        # Update schema with the transformed features names.
        # It is possible feature names change after ``fit_transform`` due to
        # one-to-many transformers/encoders.
        transformed_data = preprocessor.fit_transform(data)
        schema = self.update_schema(schema, preprocessor)

        # Create estimator.
        estimator = self.create_estimator_from_schema(schema, **trainer)
        dataset = estimator.get_dataset(transformed_data)

        # Create and set callbacks.
        skorch_callbacks = trainer.get('skorch_callbacks', [])
        lr_scheduler = trainer.get('lr_scheduler')
        skorch_callbacks = self.create_skorch_callbacks(
            skorch_callbacks, lr_scheduler, estimator, dataset)
        estimator.set_params(callbacks=skorch_callbacks)

        self.fit(dataset, preprocessor, estimator)

    def create_skorch_callbacks(
            self,
            callbacks: List[Dict],
            lr_scheduler: Optional[Dict],
            estimator: Optional[nn.TimeseriesNeuralNet],
            dataset: Optional[nn.TimeseriesDataset],
            mlflow_logger: bool = True
    ) -> List[skorch.callbacks.Callback]:
        """Creates list of skorch callbacks.

        Returns
        -------
        callbacks : list
            List of skorch callbacks.
        """
        skorch_callbacks = []
        callbacks_creator = _callbacks.CallbacksCreator()

        for callback in callbacks:
            callback = callbacks_creator.create_callback(**callback)
            skorch_callbacks.append(callback)

        if lr_scheduler is not None:
            lrs_callback = self._create_lrs_callback(
                lr_scheduler, estimator, dataset)
            skorch_callbacks.append(lrs_callback)

        if mlflow_logger:
            mlflow_callback = self._create_mlflow_callback()
            skorch_callbacks.append(mlflow_callback)

        return skorch_callbacks

    def _create_lrs_callback(self, lr_scheduler, estimator, dataset):
        lrs_creator = _callbacks.LRSCallbackCreator(estimator, dataset)
        return lrs_creator.create_callback(**lr_scheduler)

    def _create_mlflow_callback(self):
        return skorch.callbacks.MlflowLogger(
            log_on_batch_end=True, create_artifact=False)

    def create_estimator_from_schema(
            self,
            schema: schemas.FeaturesSchema,
            algorithm: str,
            max_prediction_length: int,
            hparams: Dict,
            **kwargs
    ) -> nn.TimeseriesNeuralNet:
        """Creates Estimator from features schema.

        Parameters
        ----------
        schema : FeaturesSchema
        algorithm : str
        max_prediction_length : int
        hparams: dict


        Returns
        -------
        estimator : nn.TimeseriesNeuralNet
        """
        estimator_creator = SkorchForecastingCreator.from_schema(schema)
        return estimator_creator.create_estimator(
            algorithm, max_prediction_length, hparams)
