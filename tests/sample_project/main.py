import os

import click
import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder


@click.command()
@click.option("--target", required=True, type=str, help="Target column.")
@click.option("--max_depth", type=int, default=5, help="Forest max depth.")
@click.option(
    "--train",
    type=str,
    help="Path for training data.",
    default=os.environ["train"],
)
def train(target, max_depth, train):
    X = pd.read_csv(train)
    y = X.pop(target)

    # Encode labels in target column.
    y = LabelEncoder().fit_transform(y)

    clf = RandomForestClassifier(max_depth=max_depth, random_state=0)

    with mlflow.start_run():
        clf.fit(X, y)
        input_example = X.iloc[[0]]
        mlflow.sklearn.log_model(clf, "model", input_example=input_example)


if __name__ == "__main__":
    train()
