import unittest
from os.path import abspath, dirname, join

from mlflow.projects.submitted_run import SubmittedRun

from anyforecast.models import Seq2Seq

TESTS_DIR = dirname(dirname(abspath(__file__)))
DATA_DIR = join(TESTS_DIR, "data")
STALLION_CSV = join(DATA_DIR, "stallion.csv")


EXPECTED_CMD = (
    "python train_seq2seq.py "
    f"--train {STALLION_CSV} "
    "--group-ids agency,sku "
    "--timestamp date "
    "--target volume "
    "--time-varying-known None "
    "--time-varying-unknown volume "
    "--static-categoricals agency,sku "
    "--static-reals None "
    "--max-prediction-length 6 "
    "--max-encoder-length 24 "
    "--freq MS "
    "--device cpu "
    "--max-epochs 1 "
    "--verbose 0 "
)


def get_run_cmd(run: SubmittedRun) -> str:
    """Returns the command ran by MLFlow."""
    return run.command_proc.args[-1].split("&&  ")[-1]


def get_exit_code(run: SubmittedRun) -> int:
    """Returns exit code from MLFlow run."""
    return run.command_proc.returncode


def create_seq2seq() -> Seq2Seq:
    """Creates Seq2Seq.

    The returned :class:`Seq2Seq` instance is ready to be fitted on the test
    data in ``STALLION_CSV``.
    """
    return Seq2Seq(
        train=STALLION_CSV,
        group_ids="agency,sku",
        timestamp="date",
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
        cls.model = create_seq2seq()
        cls.model.fit()
        cls.model.promise_.wait()  # Block until finish.

    def test_is_fitted(self) -> None:
        assert hasattr(self.model, "promise_")

    def test_exit_code(self) -> None:
        run = self.model.promise_.result()
        exit_code = get_exit_code(run)
        assert exit_code == 0

    def test_run_cmd(self) -> None:
        run = self.model.promise_.result()
        command = get_run_cmd(run)
        assert command == EXPECTED_CMD
