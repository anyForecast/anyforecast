import click
from cli_options import skorchforecasting_options
from preprocessing import make_preprocessor
from sklearn.pipeline import Pipeline
from skorch_forecasting.models import TemporalFusionTransformer as TFT


@click.command()
@skorchforecasting_options
def train(
    train,
    group_ids,
    timestamp,
    target,
    time_varying_known,
    time_varying_unknown,
    static_categoricals,
    max_prediction_length,
    max_encoder_length,
    device,
):
    preprocessor = make_preprocessor(target, group_ids)
    estimator = TFT(
        group_ids=group_ids,
        time_idx="time_idx",
        target=target,
        min_encoder_length=max_encoder_length // 2,
        max_encoder_length=max_encoder_length,
        min_prediction_length=1,
        max_prediction_length=max_prediction_length,
        device=device,
    )

    pipe = Pipeline([("preprocessor", preprocessor), ("estimator", estimator)])
    pipe.fit(X)


if __name__ == "__main__":
    train()
