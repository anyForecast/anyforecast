import click
import pandas as pd
from options import skorchforecasting_options
from preprocessing import make_preprocessor
from pytorch_forecasting.data.examples import get_stallion_data
from sklearn.pipeline import Pipeline
from skorch_forecasting.models import Seq2Seq


def get_sample_data() -> pd.DataFrame:
    data = get_stallion_data()

    cols = [
        "agency",
        "sku",
        "volume",
        "date",
        "industry_volume",
        "price_regular",
        "price_actual",
        "discount",
    ]

    group_ids = ["agency", "sku"]

    top_groups = (
        data.groupby(group_ids)
        .agg({"volume": "sum"})["volume"]
        .nlargest(10)
        .index.to_list()
    )

    data = data.set_index(group_ids).loc[top_groups].reset_index()
    return data[cols]


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
    static_reals,
    max_prediction_length,
    max_encoder_length,
    freq,
    device,
):
    data = get_sample_data()
    preprocessor = make_preprocessor(group_ids, timestamp, target, freq)
    estimator = Seq2Seq(
        group_ids=group_ids,
        time_idx=timestamp,
        target=target,
        time_varying_known_reals=time_varying_known,
        time_varying_unknown_reals=time_varying_unknown,
        static_categoricals=static_categoricals,
        static_reals=static_reals,
        min_encoder_length=max_encoder_length // 2,
        max_encoder_length=max_encoder_length,
        min_prediction_length=1,
        max_prediction_length=max_prediction_length,
        device=device,
    )

    pipe = Pipeline([("preprocessor", preprocessor), ("estimator", estimator)])
    pipe.fit(data)


if __name__ == "__main__":
    train()
