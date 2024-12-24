from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic import validator


class Flow(BaseModel):
    id: UUID
    name: str

    @validator("id")
    def uuid_to_hex(cls, v):
        return v.hex if v else None


class BasePrefect(BaseModel):
    # UUIDs
    id: UUID
    state_id: UUID

    # Dimensions
    name: str
    state_name: str

    # Dates
    created: datetime
    exported_at: datetime = datetime.now(timezone.utc)
    start_time: datetime
    end_time: Optional[datetime] = None

    # Timedeltas
    total_run_time: timedelta

    # Other
    empirical_policy: dict
    tags: list

    # Measures
    run_count: int

    @validator("total_run_time")
    def to_seconds(cls, v):
        return v / timedelta(seconds=1)


class FlowRun(BasePrefect):
    # UUIDs
    flow_id: UUID
    flow_version: UUID
    parent_task_run_id: Optional[UUID]

    # Other
    parameters: dict

    @validator("id", "state_id", "flow_id", "flow_version", "parent_task_run_id")
    def uuid_to_hex(cls, v):
        return v.hex if v else None

    @validator("empirical_policy", "tags", "parameters")
    def cast_to_string(cls, v):
        return str(v)


class TaskRun(BasePrefect):
    # UUIDs
    flow_run_id: UUID

    # Dimensions
    task_key: str

    # Other
    task_inputs: dict

    # Measures
    dynamic_key: int
    flow_run_run_count: int

    @validator("id", "state_id", "flow_run_id")
    def uuid_to_hex(cls, v):
        return v.hex if v else None

    @validator("empirical_policy", "tags", "task_inputs")
    def cast_to_string(cls, v):
        return str(v)
