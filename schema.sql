CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    task_key VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'waiting', -- waiting, running, completed, failed, wont_do
    input_data JSON,
    output_data JSON,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    execution_timeout_seconds INTEGER DEFAULT 600,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    lock_instance_uuid UUID,
    lock_acquired_at TIMESTAMP WITH TIME ZONE
);

-- Index for faster querying of tasks by status
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS task_dependencies (
    child_task_id INTEGER REFERENCES tasks(id),
    parent_task_id INTEGER REFERENCES tasks(id),
    PRIMARY KEY (child_task_id, parent_task_id)
);

-- Index for faster dependency lookups
CREATE INDEX IF NOT EXISTS idx_task_dependencies_child ON task_dependencies(child_task_id);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_parent ON task_dependencies(parent_task_id);