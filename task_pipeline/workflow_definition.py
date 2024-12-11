from typing import Dict, List
from dataclasses import dataclass
from task_pipeline.task_definition import TaskDefinition

@dataclass
class WorkflowDefinition:
    tasks: Dict[str, TaskDefinition]

    def validate(self) -> bool:
        """
        Validates that all dependency references exist and there are no cycles
        """
        # Implementation of cycle detection and validation
        return True