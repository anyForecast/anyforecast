from __future__ import annotations

import os
import unittest
from typing import Literal

from mlflow.projects.submitted_run import SubmittedRun

from anyforecast.estimator import MLFlowEstimator

TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = os.path.join(TESTS_DIR, "unit/projects")
IRIS_CSV = os.path.join(TESTS_DIR, "data/iris.csv")

EXPECTED_CMD = f"python main.py --train {IRIS_CSV} --max_depth 7"


def get_run_cmd(run: SubmittedRun) -> str:
    """Returns the command ran by MLFlow."""
    return run.command_proc.args[-1].split("&&  ")[-1]


def get_exit_code(run: SubmittedRun) -> int:
    """Returns exit code from MLFlow run."""
    return run.command_proc.returncode


def create_estimator() -> RandomForecastEstimator:
    return RandomForecastEstimator(train=IRIS_CSV)


class RandomForecastEstimator(MLFlowEstimator):
    def __init__(
        self,
        train : str,
        max_depth: int = 7,
        experiment_name: str | None = None,
        experiment_id: str | None = None,
        run_name: str | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] | None = None,
    ):
        self.train = train
        self.max_depth = max_depth

        super().__init__(
            project_uri=PROJECT_DIR,
            experiment_name=experiment_name,
            experiment_id=experiment_id,
            run_name=run_name,
            env_manager=env_manager,
        )

    def get_parameters(self) -> dict:
        return {"train": self.train, "max_depth": self.max_depth}


class TestEstimator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.estimator = create_estimator()
        cls.estimator.fit()

    def test_is_fitted(self) -> None:
        assert hasattr(self.estimator.run_)

    def test_exit_code(self) -> None:
        exit_code = get_exit_code(self.estimator.run_)
        assert exit_code == 0

    def test_run_cmd(self) -> None:
        command = get_run_cmd(self.estimator.run_)
        assert command == EXPECTED_CMD

    def test_is_registered(self) -> None:
        pass
