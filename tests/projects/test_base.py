from __future__ import annotations

import os
import unittest

from mlflow.projects.submitted_run import SubmittedRun

from anyforecast import backend, datasets, projects

IRIS_DS = datasets.load_iris()
TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = os.path.join(TESTS_DIR, "sample_project")


def get_run_cmd(run: SubmittedRun) -> str:
    """Returns the command ran by MLFlow."""
    return run.command_proc.args[-1].split("&& ")[-1]


def get_exit_code(run: SubmittedRun) -> int:
    """Returns exit code from MLFlow run."""
    return run.command_proc.returncode


class RandomForestProject(projects.MLFlowProject):
    """Random Forecast sample project."""

    def __init__(
        self,
        model_params: dict,
        backend_exec: backend.BackendExecutor = backend.LocalBackend(),
    ):
        super().__init__(
            uri=PROJECT_DIR,
            model_params=model_params,
            backend_exec=backend_exec,
        )


class BaseTestCases:
    class TestProject(unittest.TestCase):
        #: Default arguments.
        model_params = {"max_depth": 5, "target": IRIS_DS.target}
        backend_exec: backend.BackendExecutor = None

        @classmethod
        def setUpClass(cls):
            if cls.backend_exec is None:
                raise ValueError("``backend_exec cannot be None.")

            cls.project = RandomForestProject(
                model_params=cls.model_params,
                backend_exec=cls.backend_exec,
            )

            cls.project.run({"train": IRIS_DS.filepath})
            # cls.estimator.promise_.wait()  # Block until finish.

        def test_is_fitted(self) -> None:
            assert hasattr(self.project, "promise_")

        def test_exit_code(self) -> None:
            run = self.project.promise_.result()
            exit_code = get_exit_code(run)
            assert exit_code == 0

        def test_run_cmd(self) -> None:
            expected_cmd = (
                f"python main.py "
                f"--target {self.model_params['target']} "
                f"--max_depth {self.model_params['max_depth']}"
            )

            run = self.project.promise_.result()
            command = get_run_cmd(run)
            assert command == expected_cmd

        def test_is_registered(self) -> None:
            pass


class TestProjectOnLocalBackend(BaseTestCases.TestProject):
    backend_exec = backend.LocalBackend()


# class TestEstimatorOnRayBackend(BaseTestCases.TestEstimator):
#    backend_exec = backend.RayBackend()
#
#    @classmethod
#    def setUpClass(cls):
#        ray.init(num_cpus=2, include_dashboard=False)
#        super().setUpClass()
#
#    @classmethod
#    def tearDownClass(cls):
#        ray.shutdown()
