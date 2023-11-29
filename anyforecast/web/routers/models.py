from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

from anyforecast.models import Seq2Seq

router = APIRouter(prefix="/models", tags=["models"])


class Seq2SeqParams(BaseModel):
    train: str
    group_ids: str
    timestamp: str
    target: str
    time_varying_known: str | None = None
    time_varying_unknown: str | None = None
    static_categoricals: str | None = None
    static_reals: str | None = None
    max_prediction_length: int = 6
    max_encoder_length: int = 24
    freq: str = "D"
    device: str = "cpu"
    max_epochs: int = 10
    verbose: int = 1
    experiment_name: str | None = None
    experiment_id: str | None = None
    run_name: str | None = None
    env_manager: Literal["local", "virtualenv", "conda"] | None = None


class AsyncResponse(BaseModel):
    task_id: str


@router.post("/seq2seq", response_model=AsyncResponse)
async def seq2seq(seq2seq_params: Seq2SeqParams):
    """Trains Seq2Seq model."""
    seq2seq = Seq2Seq(**seq2seq_params.model_dump())
    promise = seq2seq.fit_async()
    return AsyncResponse(task_id=promise.task_id)
