import unittest
from typing import Any

from anyforecast_datasets.loaders import load_iris
from mlflow.projects.submitted_run import SubmittedRun

from anyforecast import backends, project, testing

IRIS_DS = load_iris()


def get_run_cmd(run: SubmittedRun) -> str:
    """Returns the command ran by MLFlow."""
    return run.command_proc.args[-1].split("&& ")[-1]


def get_exit_code(run: SubmittedRun) -> int:
    """Returns exit code from MLFlow run."""
    return run.command_proc.returncode


class RandomForestProject(project.MLflowProject):
    """Random Forecast sample project."""

    def __init__(self, target: str, max_depth: int = 5):
        super().__init__(uri=testing.PROJECT_DIR)

        self.target = target
        self.max_depth = max_depth

    def get_parameters(self) -> dict[str, Any]:
        return {"target": self.target, "max_depth": self.max_depth}


class BaseTestCases:
    class TestProject(unittest.TestCase):

        #: Default arguments.
        backend_exec: backend.BackendExecutor = None

        @classmethod
        def setUpClass(cls):
            if cls.backend_exec is None:
                raise ValueError("``backend_exec cannot be None.")

            cls.project = RandomForestProject(target=IRIS_DS.target)

            cls.project.run(
                input_channels={"train": IRIS_DS.filepath},
                backend=cls.backend_exec,
            )
            # cls.estimator.promise_.wait()  # Block until finish.

        def test_is_fitted(self) -> None:
            assert hasattr(self.project, "promise_")

        def test_exit_code(self) -> None:
            run = self.project.promise_.result()
            exit_code = get_exit_code(run)
            assert exit_code == 0

        def test_run_cmd(self) -> None:
            parameters = self.project.get_parameters()

            expected_cmd = (
                f"python main.py "
                f"--target {parameters['target']} "
                f"--max_depth {parameters['max_depth']}"
            )

            run = self.project.promise_.result()
            command = get_run_cmd(run)
            assert command == expected_cmd

        def test_is_registered(self) -> None:
            pass


class TestProjectOnLocalBackend(BaseTestCases.TestProject):
    backend_exec = backends.LocalBackend()


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
