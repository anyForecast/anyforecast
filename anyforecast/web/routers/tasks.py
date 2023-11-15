from fastapi import APIRouter
from pydantic import BaseModel

from anyforecast.web.executor import execute

router = APIRouter(prefix="/tasks", tags=["tasks"])


class Numbers(BaseModel):
    x: int
    y: int


@router.post("/add")
async def add(numbers: Numbers):
    return execute(
        name="anyforecast.tasks.examples.add", kwargs=numbers.model_dump()
    )
