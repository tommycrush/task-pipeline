from dataclasses import dataclass
from typing import Dict, Any, List, Optional

@dataclass
class TaskDefinition:
    task_key: str
    input_data: Optional[Dict[str, Any]] = None
    max_retries: int = 3
    depends_on: List[str] = None  # List of task_keys this task depends on 
    execution_timeout_seconds: int = 500  # Default timeout of 500 seconds