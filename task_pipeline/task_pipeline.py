import asyncio
import json
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
import psycopg
from task_pipeline.task import Task, TaskStatus
from task_pipeline.workflow_definition import WorkflowDefinition

class TaskPipeline:
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.instance_uuid = uuid.uuid4()

    async def peel_next_eligible_waiting_task(self, task_keys: Optional[List[str]] = None) -> Optional[Task]:
        """
        Finds and claims the next eligible waiting task.
        
        Args:
            task_keys: Optional list of task keys to filter by. If None, considers all tasks.
        
        Returns:
            Task object if an eligible task was found and claimed, None otherwise.
        """
        async with await psycopg.AsyncConnection.connect(self.db_connection_string) as conn:
            async with conn.cursor() as cur:
                await cur.execute("BEGIN")
                
                try:
                    query = """
                    WITH eligible_tasks AS (
                        SELECT t.id 
                        FROM tasks t
                        LEFT JOIN task_dependencies td ON t.id = td.child_task_id
                        LEFT JOIN tasks parent ON td.parent_task_id = parent.id
                        WHERE ((t.status = 'waiting' AND t.lock_instance_uuid IS NULL)
                           OR (t.status = 'running' AND t.retry_count < t.max_retries AND t.lock_acquired_at + (t.execution_timeout_seconds || ' seconds')::INTERVAL < CURRENT_TIMESTAMP ))
                        AND (t.task_key = ANY(%s) OR %s)
                        GROUP BY t.id
                        HAVING BOOL_AND(parent.status = 'completed' OR parent.id IS NULL)
                        LIMIT 1
                    )
                    UPDATE tasks t
                    SET status = 'running',
                        lock_instance_uuid = %s,
                        lock_acquired_at = CURRENT_TIMESTAMP,
                        retry_count = retry_count + 1
                    WHERE id = (SELECT id FROM eligible_tasks)
                    RETURNING t.id, t.task_key, t.status, t.input_data, t.output_data, 
                              t.error_message, t.retry_count, t.max_retries, t.created_at,
                              t.lock_instance_uuid, t.lock_acquired_at;
                    """
                    # Convert None to NULL for PostgreSQL with explicit type casting
                    task_keys_param = []
                    if task_keys is not None:
                            task_keys_param
                    await cur.execute(query, (task_keys_param, task_keys is None, self.instance_uuid))
                    row = await cur.fetchone()
                    
                    if row:
                        columns = [desc[0] for desc in cur.description]
                        row_dict_of_task = dict(zip(columns, row))
                        #print("row_dict_of_task: ", row_dict_of_task)
                        
                        parent_query = """
                        SELECT parent.task_key, parent.output_data
                        FROM tasks t
                        JOIN task_dependencies td ON t.id = td.child_task_id
                        JOIN tasks parent ON td.parent_task_id = parent.id
                        WHERE t.id = %s AND parent.status = 'completed'
                        """
                        await cur.execute(parent_query, (row[0],))  # row[0] is task id
                        parent_rows = await cur.fetchall()
                        
                        columns_from_parent_query = [desc[0] for desc in cur.description]
                        row_dict_from_parent_query = dict(zip(columns_from_parent_query, parent_rows))
                        
                        parent_output_data = {}
                        for parent_row in parent_rows:
                            if parent_row[1] is not None:
                                task_key = parent_row[0]
                                output_data = parent_row[1] if isinstance(parent_row[1], dict) else json.loads(parent_row[1])
                                parent_output_data[task_key] = output_data
                        
                        row_dict_of_task['parent_output_data'] = parent_output_data
                        await conn.commit()
                        return Task.from_db_row(row_dict_of_task)
                    
                    await conn.commit()
                    return None
                    
                except Exception as e:
                    await conn.rollback()
                    raise e

    async def mark_task_as_completed(self, task_id: int, output_data: Dict[str, Any]) -> None:
        """
        Marks a task as completed and stores its output data.
        """
        print(output_data)
        async with await psycopg.AsyncConnection.connect(self.db_connection_string) as conn:
            async with conn.cursor() as cur:
                query = """
                UPDATE tasks
                SET status = 'completed',
                    output_data = %s,
                    lock_instance_uuid = NULL,
                    lock_acquired_at = NULL
                WHERE id = %s AND lock_instance_uuid = %s
                """
                # Serialize the output_data to JSON string
                json_output_data = json.dumps(output_data)
                await cur.execute(query, (json_output_data, task_id, self.instance_uuid))
                await conn.commit()

    async def mark_task_as_failed(self, task_id: int, error_message: str, is_system_cleanup: bool = False) -> None:
        """
        Marks a task as failed if retry count is over the max_retries,
        otherwise marks as permanently failed and marks all dependent tasks as 'wont_do'.
        
        Args:
            task_id: The ID of the task to mark as failed
            error_message: The error message to store
            is_system_cleanup: If True, skips lock_instance_uuid check for system cleanup operations
        """
        print(f"Marking task {task_id} as failed with error message: {error_message}")
        async with await psycopg.AsyncConnection.connect(self.db_connection_string) as conn:
            async with conn.cursor() as cur:
                # Begin transaction
                await cur.execute("BEGIN")
                
                try:
                    # Get current retry information
                    await cur.execute(
                        "SELECT retry_count, max_retries FROM tasks WHERE id = %s",
                        (task_id,)
                    )
                    retry_count, max_retries = await cur.fetchone()
                    
                    new_status = 'waiting' if retry_count < max_retries else 'failed'
                    print(new_status)
                    
                    # Build the WHERE clause based on is_system_cleanup
                    where_clause = "WHERE id = %s"
                    query_params = [new_status, error_message, task_id]
                    
                    if not is_system_cleanup:
                        where_clause += " AND lock_instance_uuid = %s"
                        query_params.append(self.instance_uuid)
                    
                    # Update the failed task
                    await cur.execute(f"""
                        UPDATE tasks
                        SET status = %s,
                            error_message = %s,
                            lock_instance_uuid = NULL,
                            lock_acquired_at = NULL
                        {where_clause}
                    """, query_params)

                    # If task has permanently failed, mark all dependent tasks as 'wont_do'
                    if new_status == 'failed':
                        await cur.execute("""
                            WITH RECURSIVE dependent_tasks AS (
                                -- Base case: direct children
                                SELECT child_task_id as id
                                FROM task_dependencies
                                WHERE parent_task_id = %s
                                
                                UNION
                                
                                -- Recursive case: children of children
                                SELECT td.child_task_id as id
                                FROM task_dependencies td
                                INNER JOIN dependent_tasks dt ON td.parent_task_id = dt.id
                            )
                            UPDATE tasks
                            SET status = 'wont_do',
                                error_message = %s,
                                lock_instance_uuid = NULL,
                                lock_acquired_at = NULL
                            WHERE id IN (SELECT id FROM dependent_tasks)
                            AND status NOT IN ('completed', 'failed', 'wont_do')
                        """, (task_id, f"Parent task {task_id} failed permanently: {error_message}"))

                    await conn.commit()
                    
                except Exception as e:
                    await conn.rollback()
                    raise e
    
    async def save_workflow_to_database(self, workflow: WorkflowDefinition) -> Dict[str, int]:
        """
        Creates a workflow of tasks from a workflow definition.
        Returns a dictionary mapping task_keys to their database IDs.
        """
        async with await psycopg.AsyncConnection.connect(self.db_connection_string) as conn:
            async with conn.cursor() as cur:
                await cur.execute("BEGIN")
                
                try:
                    # First, create all tasks. 
                    task_ids = {} 
                    for task_key, task_def in workflow.tasks.items():
                        await cur.execute("""
                            INSERT INTO tasks (task_key, input_data, max_retries, execution_timeout_seconds)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (task_key, json.dumps(task_def.input_data), task_def.max_retries, task_def.execution_timeout_seconds))
                        task_ids[task_key] = (await cur.fetchone())[0]
                    
                    # Then create all dependencies
                    for task_key, task_def in workflow.tasks.items():
                        if task_def.depends_on:
                            for parent_key in task_def.depends_on:
                                await cur.execute("""
                                    INSERT INTO task_dependencies (child_task_id, parent_task_id)
                                    VALUES (%s, %s)
                                """, (task_ids[task_key], task_ids[parent_key]))
                    
                    await conn.commit()
                    return task_ids
                    
                except Exception as e:
                    await conn.rollback()
                    raise e

    async def log_tasks(self) -> None:
        """
        Displays the latest 20 tasks in a formatted table in the console.
        """
        try:
            from tabulate import tabulate
        except ImportError:
            raise ImportError("Please install tabulate: pip install tabulate")

        async with await psycopg.AsyncConnection.connect(self.db_connection_string) as conn:
            async with conn.cursor() as cur:
                query = """
                SELECT id, task_key, status, retry_count, max_retries,
                       error_message, created_at, lock_acquired_at, lock_acquired_at + (execution_timeout_seconds || ' seconds')::INTERVAL AS execution_timeout_date, CURRENT_TIMESTAMP AS current_timestamp, execution_timeout_seconds
                FROM tasks
                ORDER BY created_at DESC
                LIMIT 20
                """
                await cur.execute(query)
                rows = await cur.fetchall()
                
                # Format the data for tabulate
                headers = ['ID', 'Task Key', 'Status', 'Retries', 'Max Retries', 
                          'Error', 'Created At', 'Locked At', 'Execution Timeout Date', 'Current Timestamp', 'Execution Timeout Seconds']
                
                # Process the rows to make them more readable
                formatted_rows = []
                for row in rows:
                    formatted_row = list(row)
                    # Truncate error message if it's too long
                    if row[5]:  # error_message
                        formatted_row[5] = row[5][:50] + '...' if len(row[5]) > 50 else row[5]
                    # Format datetime objects
                    formatted_row[6] = row[6].strftime('%Y-%m-%d %H:%M:%S')  # created_at
                    formatted_row[7] = row[7].strftime('%Y-%m-%d %H:%M:%S') if row[7] else '-'  # lock_acquired_at
                    formatted_rows.append(formatted_row)

                # Print the table
                print("\nLatest Tasks:")
                print(tabulate(formatted_rows, headers=headers, tablefmt='grid'))

    async def mark_hanging_jobs_as_failed(self) -> Optional[List[int]]:
        """
        Identifies and marks tasks as failed if they:
        1. Are in 'running' state
        2. Have exceeded their execution timeout
        3. Have no more retries available
        """
        async with await psycopg.AsyncConnection.connect(self.db_connection_string) as conn:
            async with conn.cursor() as cur:
                # Find hanging tasks
                query = """
                SELECT id 
                FROM tasks 
                WHERE status = 'running'
                AND lock_acquired_at + (execution_timeout_seconds || ' seconds')::INTERVAL < CURRENT_TIMESTAMP
                AND retry_count >= max_retries
                """
                await cur.execute(query)
                hanging_tasks = await cur.fetchall()
                
                # Mark each hanging task as failed
                for task_id, in hanging_tasks:
                    await self.mark_task_as_failed(
                        task_id=task_id,
                        error_message="Task exceeded execution timeout and max attempts",
                        is_system_cleanup=True
                    )
                # Return the task_ids
                return [task_id for task_id, in hanging_tasks]
