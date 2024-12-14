import asyncio
from task_pipeline.task_pipeline import TaskPipeline
from task_pipeline.task_definition import TaskDefinition
from task_pipeline.workflow_definition import WorkflowDefinition
from task_pipeline.task import Task, TaskStatus
import random

async def process_task(pipeline: TaskPipeline, task: Task):
    """Process a single task. Override this with your actual task processing logic.
        Realistically you should import your task handling logic and call the method here.
    """
    try:
        # Example task processing based on task_key
        print("Running task", task)
        if task.task_key == "fetch_data":
            # Simulate fetching data
            await asyncio.sleep(3) # this will cause the task to timeout
            output_data = {"data": "Sample fetched data", "random_number": random.randint(1, 100)} 
            await pipeline.mark_task_as_completed(task.id, output_data)

        elif task.task_key == "process_data":
            # Simulate processing data
            await asyncio.sleep(1)
            # Access input_data safely with get() method or use empty dict as fallback
            input_data = task.input_data if task.input_data else {}
            output_data = {"processed": f"Processed: {input_data}"}
            await pipeline.mark_task_as_completed(task.id, output_data)
            
        elif task.task_key == "generate_report":
            # Simulate report generation
            await asyncio.sleep(1)
            output_data = {"report": "Final report generated"}
            await pipeline.mark_task_as_completed(task.id, output_data)
        else:
            print("Unknown task with key", task.task_key)
        
    except Exception as e:
        error_message = str(e)
        print('Error in process_task: ', e)
        await pipeline.mark_task_as_failed(task.id, error_message)
        raise  # Re-raise the exception to bubble it up during development

async def create_example_workflow(pipeline: TaskPipeline):
    """Creates an example workflow with multiple dependent tasks."""
    workflow = WorkflowDefinition(tasks={
        "fetch_data": TaskDefinition(
            task_key="fetch_data",
            input_data={"source": "example_api"},
            max_retries=3,
            execution_timeout_seconds=1
        ),
        "process_data": TaskDefinition(
            task_key="process_data",
            input_data={"something_else": "other data"},
            depends_on=["fetch_data"],
            max_retries=2,
            execution_timeout_seconds=60  # 10 minutes timeout
        ),
        "generate_report": TaskDefinition(
            task_key="generate_report",
            depends_on=["process_data"],
            max_retries=1
        )
    })
    
    return await pipeline.save_workflow_to_database(workflow)

async def main():
    # Connection string for local PostgreSQL database
    DB_CONNECTION_STRING = "postgresql://tcrush@localhost/simple_async_task_pipeline"
    
    # Initialize pipeline
    pipeline = TaskPipeline(DB_CONNECTION_STRING)
    
    # Create example workflow
    print("Creating example workflow...")
    task_ids = await create_example_workflow(pipeline)
    print(f"Created workflow with task IDs: {task_ids}")
    
    # Start worker loop
    print("Starting worker loop...")
    await worker_loop(pipeline)

async def worker_loop(pipeline: TaskPipeline):

    num_iteration = 0;
    while True:
        num_iteration += 1;
        try:
            if num_iteration % 10 == 0:
                task_ids = await pipeline.mark_hanging_jobs_as_failed()
                if task_ids:
                    print(f"Marked {len(task_ids)} tasks as failed: {task_ids}")
            # Log the latest tasks in the database for debugging
            # await pipeline.log_tasks()

            task = await pipeline.peel_next_eligible_waiting_task()
            if task:
                print(f"Processing task {task.task_key} iteration {num_iteration}")
                await process_task(pipeline, task)
            else:
                print("No eligible tasks found, waiting...")
            
            await asyncio.sleep(1)
            
        except Exception as e:
            print(e)
            print(f"Error in worker loop: {e}")
            await asyncio.sleep(10)
            raise  # Re-raise the exception to bubble it up during development

if __name__ == "__main__":
    asyncio.run(main())
