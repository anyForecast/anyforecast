import unittest

from anyforecast_datasets.loaders import load_stallion
from mlflow.projects.submitted_run import SubmittedRun

from anyforecast.projects import Seq2SeqProject

STALLION_DS = load_stallion()


def get_run_cmd(run: SubmittedRun) -> str:
    """Returns the command ran by MLFlow."""
    return run.command_proc.args[-1].split("&&  ")[-1]


def get_exit_code(run: SubmittedRun) -> int:
    """Returns exit code from MLFlow run."""
    return run.command_proc.returncode


def create_seq2seq_project() -> Seq2SeqProject:
    """Creates Seq2SeqProject.|

    The returned :class:`Seq2SeqProject` instance is ready to be fitted on the
    ``STALLION_CSV``.
    """
    return Seq2SeqProject(
        group_cols="agency,sku",
        datetime="date",
        target="volume",
        time_varying_unknown="volume",
        static_categoricals="agency,sku",
        freq="MS",
        max_epochs=1,
        verbose=0,
    )


class TestSeq2Seq(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project = create_seq2seq_project()
        cls.project.run(input_channels={"train": STALLION_DS.filepath})
        cls.project.promise_.wait()  # Block until finish.

    def test_is_fitted(self) -> None:
        assert hasattr(self.project, "promise_")

    def test_exit_code(self) -> None:
        run = self.project.promise_.result()
        exit_code = get_exit_code(run)
        assert exit_code == 0

    def test_run_cmd(self) -> None:
        expected_cmd = (
            "python train.py "
            "--group_cols agency,sku "
            "--datetime date "
            "--target volume "
            "--time_varying_known None "
            "--time_varying_unknown volume "
            "--static_categoricals agency,sku "
            "--static_reals None "
            "--max_prediction_length 6 "
            "--max_encoder_length 24 "
            "--freq MS "
            "--device cpu "
            "--max_epochs 1 "
            "--verbose 0 "
        )

        run = self.project.promise_.result()
        command = get_run_cmd(run)
        assert command == expected_cmd
