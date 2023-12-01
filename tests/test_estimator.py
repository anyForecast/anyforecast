from __future__ import annotations

import os
import unittest
from typing import Literal

import ray
from mlflow.projects.submitted_run import SubmittedRun

from anyforecast.backend import BackendExecutor, LocalBackend, RayBackend
from anyforecast.callbacks import Callback
from anyforecast.estimator import MLFlowEstimator

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(TESTS_DIR, "projects")

TRAIN = os.path.join(TESTS_DIR, "data/iris.csv")
MAX_DEPTH = 5
TARGET = "species"

EXPECTED_CMD = (
    f"python main.py --train {TRAIN} --max-depth {MAX_DEPTH} --target {TARGET}"
)


def get_run_cmd(run: SubmittedRun) -> str:
    """Returns the command ran by MLFlow."""
    return run.command_proc.args[-1].split("&& ")[-1]


def get_exit_code(run: SubmittedRun) -> int:
    """Returns exit code from MLFlow run."""
    return run.command_proc.returncode


def create_estimator(backend_exec: BackendExecutor) -> RandomForestEstimator:
    return RandomForestEstimator(
        train=TRAIN,
        max_depth=MAX_DEPTH,
        target=TARGET,
        backend_exec=backend_exec,
    )


class RandomForestEstimator(MLFlowEstimator):
    def __init__(
        self,
        train: str,
        target: str,
        max_depth: int = 7,
        experiment_name: str | None = None,
        experiment_id: str | None = None,
        run_name: str | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] | None = None,
        callbacks: list[Callback] = (),
        backend_exec: BackendExecutor = LocalBackend(),
    ):
        self.train = train
        self.target = target
        self.max_depth = max_depth

        super().__init__(
            project_uri=PROJECT_DIR,
            experiment_name=experiment_name,
            experiment_id=experiment_id,
            run_name=run_name,
            env_manager=env_manager,
            callbacks=callbacks,
            backend_exec=backend_exec,
        )

    def get_parameters(self) -> dict:
        return {
            "train": self.train,
            "target": self.target,
            "max-depth": self.max_depth,
        }


class BaseTestCases:
    class TestEstimator(unittest.TestCase):
        backend_exec: BackendExecutor = None

        @classmethod
        def setUpClass(cls):
            if cls.backend_exec is None:
                raise ValueError("``backend_exec cannot be None.")

            cls.estimator = create_estimator(backend_exec=cls.backend_exec)
            cls.estimator.fit()
            #cls.estimator.promise_.wait()  # Block until finish.

        def test_is_fitted(self) -> None:
            assert hasattr(self.estimator, "promise_")

        def test_exit_code(self) -> None:
            run = self.estimator.promise_.result()
            exit_code = get_exit_code(run)
            assert exit_code == 0

        def test_run_cmd(self) -> None:
            run = self.estimator.promise_.result()
            command = get_run_cmd(run)
            assert command == EXPECTED_CMD

        def test_is_registered(self) -> None:
            pass


class TestEstimatorOnLocalBackend(BaseTestCases.TestEstimator):
    backend_exec = LocalBackend()


class TestEstimatorOnRayBackend(BaseTestCases.TestEstimator):
    backend_exec = RayBackend()

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2, include_dashboard=False)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
