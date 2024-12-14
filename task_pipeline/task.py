from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID
from enum import Enum

class TaskStatus(Enum):
    WAITING = "waiting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass(frozen=True)
class Task:
    id: int
    task_key: str
    status: TaskStatus
    input_data: Optional[Dict[str, Any]]
    output_data: Optional[Dict[str, Any]]
    parent_output_data: Dict[str, Dict[str, Any]]
    error_message: Optional[str]
    retry_count: int
    max_attempts: int
    created_at: datetime
    lock_instance_uuid: Optional[UUID]
    lock_acquired_at: Optional[datetime]

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Task':
        """
        Create a Task instance from a database row dictionary.
        """
        return cls(
            id=row['id'],
            task_key=row['task_key'],
            status=TaskStatus(row['status']),
            input_data=row['input_data'],
            output_data=row['output_data'],
            parent_output_data=row.get('parent_output_data', {}),
            error_message=row['error_message'],
            retry_count=row['retry_count'],
            max_attempts=row['max_attempts'],
            created_at=row['created_at'],
            lock_instance_uuid=row['lock_instance_uuid'],
            lock_acquired_at=row['lock_acquired_at']
        )
