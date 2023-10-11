import functools

import click


def split_commas(ctx, param, value: str) -> list[str]:
    """Splits commas. Used as callback in click Options."""
    if not value or value is None:
        return []

    return value.split(",")


def skorchforecasting_options(f: callable):
    @click.option(
        "--train", required=True, type=str, help="Filepath for training data."
    )
    @click.option(
        "--group-ids",
        required=True,
        type=str,
        callback=split_commas,
        help="[Comma separated] Column names identifying a time series. If you "
        "have only one time series, set this to the name of column that is "
        "constant. ",
    )
    @click.option(
        "--timestamp",
        required=True,
        type=str,
        help="Timestamp or datetime column. The ``group-ids`` columns identify a "
        "sample together with the ``timestamp``.",
    )
    @click.option(
        "--target",
        required=True,
        type=str,
        help="Column denoting the target.",
    )
    @click.option(
        "--time-varying-known",
        type=str,
        callback=split_commas,
        help="[Comma separated] Continuous variables that change over time and are "
        "known in the future (e.g. price of a product, but not demand of a "
        "product).",
    )
    @click.option(
        "--time-varying-unknown",
        type=str,
        callback=split_commas,
        help="[Comma separated] Continuous variables that change over time and are "
        "not known in the future.",
    )
    @click.option(
        "--static-categoricals",
        type=str,
        callback=split_commas,
        help="[Comma separated] Categorical variables that do not change over "
        "time.",
    )
    @click.option(
        "--static-reals",
        type=str,
        callback=split_commas,
        help="[Comma separated] Continuous variables that do not change over "
        "time.",
    )
    @click.option(
        "--max-prediction-length",
        default=6,
        type=int,
        help="Maximum prediction/decoder length (choose this not too short as it "
        "can help convergence)",
    )
    @click.option(
        "--max-encoder-length",
        default=24,
        type=int,
        help="Maximum length to encode. This is the maximum history length used by "
        "the time series dataset.",
    )
    @click.option(
        "--freq",
        default="D",
        type=str,
        help="Time series frequency. Must be a valid pandas frequency.",
    )
    @click.option(
        "--device",
        default="cpu",
        type=str,
        help="The compute device to be used.",
    )
    @functools.wraps(f)
    def wrapper_common_options(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper_common_options
