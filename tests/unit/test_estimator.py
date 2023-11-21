from __future__ import annotations

import unittest
from os.path import abspath, dirname, join
from typing import Literal

from mlflow.projects.submitted_run import SubmittedRun

from anyforecast.estimator import MLFlowEstimator

TESTS_DIR = dirname(dirname(dirname(abspath(__file__))))
PROJECT_DIR = join(TESTS_DIR, "project")

EXPECTED_CMD = "python main.py " f"--train {STALLION_CSV} " "--max_depth 7"


def get_run_cmd(run: SubmittedRun) -> str:
    """Returns the command ran by MLFlow."""
    return run.command_proc.args[-1].split("&&  ")[-1]


def get_exit_code(run: SubmittedRun) -> int:
    """Returns exit code from MLFlow run."""
    return run.command_proc.returncode


def create_estimator() -> RandomForecastEstimator:
    return RandomForecastEstimator()


class RandomForecastEstimator(MLFlowEstimator):
    def __init__(
        self,
        max_depth: int = 7,
        experiment_name: str | None = None,
        experiment_id: str | None = None,
        run_name: str | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] | None = None,
    ):
        self.max_depth = max_depth

        super().__init__(
            project_uri=PROJECT_DIR,
            experiment_name=experiment_name,
            experiment_id=experiment_id,
            run_name=run_name,
            env_manager=env_manager,
        )

    def get_parameters(self) -> dict:
        return {"max_depth": self.max_depth}


class TestEstimator(unittest.TestCase):
    def setUp(self) -> None:
        self.estimator = create_estimator()
        self.estimator.fit()

    def test_exit_code(self) -> None:
        exit_code = get_exit_code(self.estimator.run_)
        assert exit_code == 0

    def test_run_cmd(self) -> None:
        command = get_run_cmd(self.estimator.run_)
        assert command == EXPECTED_CMD

    def test_is_registered(self) -> None:
        pass
