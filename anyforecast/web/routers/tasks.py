from fastapi import APIRouter
from pydantic import BaseModel

from anyforecast.web.executor import execute_task

router = APIRouter(prefix="/tasks", tags=["tasks"])


class Numbers(BaseModel):
    x: int
    y: int


@router.post("/add")
async def add(numbers: Numbers):
    promise = execute_task(
        name="anyforecast.tasks.examples.add", kwargs=numbers.model_dump()
    )

    return {"task_id": promise.task_id}
