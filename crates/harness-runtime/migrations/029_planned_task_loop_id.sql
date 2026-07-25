-- Migration 029: Add materialized_loop_id to planned_tasks for I7 goal orchestration.
-- Tracks the I4.5 TaskEngineeringLoop that was created for this planned task,
-- enabling the GoalLoop to poll task status and import observations.

ALTER TABLE planned_tasks ADD COLUMN materialized_loop_id TEXT;

CREATE INDEX IF NOT EXISTS idx_planned_task_loop_id ON planned_tasks(materialized_loop_id);
