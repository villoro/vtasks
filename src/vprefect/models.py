from datetime import datetime, timedelta
from uuid import UUID

from pydantic import BaseModel, validator


class Flow(BaseModel):
    # UUIDs
    id: UUID
    flow_id: UUID
    state_id: UUID
    flow_version: UUID
    parent_task_run_id: UUID
    # Dimensions
    name: str
    state_name: str
    # Dates
    created: datetime
    exported_at: datetime = datetime.now()
    start_time: datetime
    end_time: datetime
    # Timedeltas
    total_run_time: timedelta
    # Other
    parameters: dict
    empirical_policy: dict
    tags: list
    run_count: int

    @validator("id", "flow_id", "state_id", "flow_version", "parent_task_run_id")
    def uuid_to_hex(cls, v):
        return v.hex

    @validator("parameters", "empirical_policy", "tags")
    def cast_to_string(cls, v):
        return str(v)

    @validator("total_run_time")
    def to_seconds(cls, v):
        return v / timedelta(seconds=1)


class Task(BaseModel):
    # UUIDs
    id: UUID
    flow_run_id: UUID
    state_id: UUID
    # Dimensions
    name: str
    task_key: str
    state_name: str
    # Dates
    created: datetime
    exported_at: datetime = datetime.now()
    start_time: datetime
    end_time: datetime
    # Timedeltas
    total_run_time: timedelta
    # Other
    dynamic_key: int
    empirical_policy: dict
    tags: list
    task_inputs: dict
    run_count: int
    flow_run_run_count: int

    @validator("id", "flow_run_id")
    def uuid_to_hex(cls, v):
        return v.hex

    @validator("empirical_policy", "tags", "task_inputs")
    def cast_to_string(cls, v):
        return str(v)

    @validator("total_run_time")
    def to_seconds(cls, v):
        return v / timedelta(seconds=1)
