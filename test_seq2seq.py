from anyforecast.ml.estimator import Estimator


def test_train_seq2seq():
    project = "skorchforecasting"
    parameters = {
        "group-ids": "agency,sku",
        "timestamp": "date",
        "target": "volume",
        "time-varying-unknown": "volume",
        "freq": "MS",
    }

    inputs = {"train": "train.csv"}
    estimator = Estimator(project=project, parameters=parameters)
    estimator.fit(inputs)
    return estimator


if __name__ == "__main__":
    estimator = test_train_seq2seq()
