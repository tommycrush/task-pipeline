# What is TaskPipeline?

TaskPipeline is a simple open source framework to manage distributed asynchronous tasks. It's designed specifically to be a starting-point for small teams that want a simple and extensible framework to get started managing resilient workflows, but don't yet want the complexity of a full-fledged workflow management system (Airflow, etc). This was inspired by needing resilient multi-step orchestration for AI/LLM workflows, but will work for async tasks generally.

## Features

TaskPipeline has all the standard features of a task management system, including the ability to:

1. Define dependencies between tasks
2. Pipe output from one task to the input of another
3. Process non-blocking tasks in parallel
4. Retry failed tasks

It's designed to be simple, and to be easy to understand and extend. This allows you to use it as a starting-point for your own workflows, and to extend it as your needs grow.

## Core Components

The core components of the framework are:

1) The "tasks" database table, which contains all the tasks that need to be run.
2) A TaskPipeline object, which is the main interface for managing tasks.
3) A Task object, which is used to represent a task that is scheduled be run.
4) A TaskDefinition object, which is used to define the tasks that need to be scheduled.
5) A WorkflowDefinition object, which is used to define the relationships between tasks.

TaskPipeline is designed to be set up once in a given process, with a centralized peeler that can then federate task execution to multiple threads. It is safe to run multiple processes in parallel, each with its own TaskPipeline object.

You own the execution loop, and can implement your own logic for how tasks are processed.

## Example

See the example_worker.py file for an example of how to use the framework.

## Future Improvements

1. Allow multiple tasks (not just 1) with the same task_key to be a dependency of a child.