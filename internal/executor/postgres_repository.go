package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateRun(ctx context.Context, run Run) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workflow_runs (
			id, tenant_id, workflow_id, status, trigger_mode, attempt, max_attempts,
			input_json, output_json, error_message, created_at, started_at, completed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`, run.ID, run.TenantID, run.WorkflowID, run.Status, run.TriggerMode, run.Attempt, run.MaxAttempts,
		mustJSON(run.Input), nullableJSON(run.Output), run.Error, run.CreatedAt, run.StartedAt, nullableTime(run.CompletedAt))
	if err != nil {
		return fmt.Errorf("insert workflow run: %w", err)
	}

	return nil
}

func (r *PostgresRepository) UpdateRun(ctx context.Context, run Run) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE workflow_runs
		SET status = $3,
		    trigger_mode = $4,
		    attempt = $5,
		    max_attempts = $6,
		    input_json = $7,
		    output_json = $8,
		    error_message = $9,
		    created_at = $10,
		    started_at = $11,
		    completed_at = $12
		WHERE id = $1 AND tenant_id = $2
	`, run.ID, run.TenantID, run.Status, run.TriggerMode, run.Attempt, run.MaxAttempts,
		mustJSON(run.Input), nullableJSON(run.Output), run.Error, run.CreatedAt, run.StartedAt, nullableTime(run.CompletedAt))
	if err != nil {
		return fmt.Errorf("update workflow run: %w", err)
	}

	return nil
}

func (r *PostgresRepository) CompleteRun(ctx context.Context, run Run, steps []Step) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin complete workflow run transaction: %w", err)
	}
	defer tx.Rollback()

	if err := updateRunTx(ctx, tx, run); err != nil {
		return err
	}
	if err := replaceStepsTx(ctx, tx, run.ID, steps); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_run_pending WHERE run_id = $1`, run.ID); err != nil {
		return fmt.Errorf("delete workflow pending state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit complete workflow run transaction: %w", err)
	}

	return nil
}

func (r *PostgresRepository) SavePending(ctx context.Context, run Run, steps []Step, pending pendingState) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin save pending workflow run transaction: %w", err)
	}
	defer tx.Rollback()

	if err := updateRunTx(ctx, tx, run); err != nil {
		return err
	}
	if err := replaceStepsTx(ctx, tx, run.ID, steps); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO workflow_run_pending (
			run_id, tenant_id, waiting_node_id, queue_json, step_outputs_json, executed_json
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (run_id) DO UPDATE
		SET tenant_id = EXCLUDED.tenant_id,
		    waiting_node_id = EXCLUDED.waiting_node_id,
		    queue_json = EXCLUDED.queue_json,
		    step_outputs_json = EXCLUDED.step_outputs_json,
		    executed_json = EXCLUDED.executed_json,
		    updated_at = NOW()
	`, run.ID, run.TenantID, pending.WaitingNodeID, mustJSON(pending.Queue), mustJSON(pending.StepOutputs), mustJSON(pending.Executed))
	if err != nil {
		return fmt.Errorf("upsert workflow pending state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit save pending workflow run transaction: %w", err)
	}

	return nil
}

func (r *PostgresRepository) LoadPending(ctx context.Context, tenantID, runID string) (Run, []Step, pendingState, error) {
	run, err := r.GetRun(ctx, tenantID, runID)
	if err != nil {
		return Run{}, nil, pendingState{}, err
	}

	steps, err := r.ListSteps(ctx, tenantID, runID)
	if err != nil {
		return Run{}, nil, pendingState{}, err
	}

	row := r.db.QueryRowContext(ctx, `
		SELECT waiting_node_id, queue_json, step_outputs_json, executed_json
		FROM workflow_run_pending
		WHERE tenant_id = $1 AND run_id = $2
	`, tenantID, runID)

	var (
		pending         pendingState
		queueJSON       []byte
		stepOutputsJSON []byte
		executedJSON    []byte
	)
	if err := row.Scan(&pending.WaitingNodeID, &queueJSON, &stepOutputsJSON, &executedJSON); err != nil {
		if err == sql.ErrNoRows {
			return Run{}, nil, pendingState{}, ErrRunNotAwaitingReview
		}
		return Run{}, nil, pendingState{}, fmt.Errorf("query workflow pending state: %w", err)
	}
	if err := json.Unmarshal(queueJSON, &pending.Queue); err != nil {
		return Run{}, nil, pendingState{}, fmt.Errorf("decode pending queue: %w", err)
	}
	if len(stepOutputsJSON) > 0 && string(stepOutputsJSON) != "null" {
		if err := json.Unmarshal(stepOutputsJSON, &pending.StepOutputs); err != nil {
			return Run{}, nil, pendingState{}, fmt.Errorf("decode pending step outputs: %w", err)
		}
	}
	if len(executedJSON) > 0 && string(executedJSON) != "null" {
		if err := json.Unmarshal(executedJSON, &pending.Executed); err != nil {
			return Run{}, nil, pendingState{}, fmt.Errorf("decode pending executed state: %w", err)
		}
	}

	return run, steps, pending, nil
}

func (r *PostgresRepository) GetRun(ctx context.Context, tenantID, runID string) (Run, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, workflow_id, status, trigger_mode, attempt, max_attempts,
		       input_json, output_json, error_message, created_at, started_at, completed_at
		FROM workflow_runs
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, runID)

	run, err := scanRun(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return Run{}, ErrNotFound
		}
		return Run{}, fmt.Errorf("query workflow run by id: %w", err)
	}

	return run, nil
}

func (r *PostgresRepository) ListRuns(ctx context.Context, tenantID string, status RunStatus) ([]Run, error) {
	query := `
		SELECT id, tenant_id, workflow_id, status, trigger_mode, attempt, max_attempts,
		       input_json, output_json, error_message, created_at, started_at, completed_at
		FROM workflow_runs
		WHERE tenant_id = $1
	`
	args := []any{tenantID}
	if status != "" {
		query += ` AND status = $2`
		args = append(args, status)
	}
	query += ` ORDER BY created_at DESC, id DESC`

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query workflow runs: %w", err)
	}
	defer rows.Close()

	runs := make([]Run, 0)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan workflow run: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow runs: %w", err)
	}

	return runs, nil
}

func (r *PostgresRepository) ListSteps(ctx context.Context, tenantID, runID string) ([]Step, error) {
	if _, err := r.GetRun(ctx, tenantID, runID); err != nil {
		return nil, err
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, run_id, node_id, node_type, status, attempt,
		       input_json, output_json, error_message, started_at, completed_at
		FROM workflow_steps
		WHERE run_id = $1
		ORDER BY started_at ASC, id ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("query workflow steps: %w", err)
	}
	defer rows.Close()

	steps := make([]Step, 0)
	for rows.Next() {
		step, err := scanStep(rows)
		if err != nil {
			return nil, fmt.Errorf("scan workflow step: %w", err)
		}
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow steps: %w", err)
	}

	return steps, nil
}

type runScanner interface {
	Scan(dest ...any) error
}

type stepScanner interface {
	Scan(dest ...any) error
}

func scanRun(scanner runScanner) (Run, error) {
	var (
		run           Run
		inputJSON     []byte
		outputJSON    []byte
		completedAt   sql.NullTime
	)
	if err := scanner.Scan(
		&run.ID,
		&run.TenantID,
		&run.WorkflowID,
		&run.Status,
		&run.TriggerMode,
		&run.Attempt,
		&run.MaxAttempts,
		&inputJSON,
		&outputJSON,
		&run.Error,
		&run.CreatedAt,
		&run.StartedAt,
		&completedAt,
	); err != nil {
		return Run{}, err
	}
	if err := decodeJSON(inputJSON, &run.Input); err != nil {
		return Run{}, err
	}
	if err := decodeNullableJSON(outputJSON, &run.Output); err != nil {
		return Run{}, err
	}
	if completedAt.Valid {
		run.CompletedAt = completedAt.Time
	}

	return run, nil
}

func scanStep(scanner stepScanner) (Step, error) {
	var (
		step        Step
		inputJSON   []byte
		outputJSON  []byte
		completedAt sql.NullTime
	)
	if err := scanner.Scan(
		&step.ID,
		&step.RunID,
		&step.NodeID,
		&step.NodeType,
		&step.Status,
		&step.Attempt,
		&inputJSON,
		&outputJSON,
		&step.Error,
		&step.StartedAt,
		&completedAt,
	); err != nil {
		return Step{}, err
	}
	if err := decodeJSON(inputJSON, &step.Input); err != nil {
		return Step{}, err
	}
	if err := decodeNullableJSON(outputJSON, &step.Output); err != nil {
		return Step{}, err
	}
	if completedAt.Valid {
		step.CompletedAt = completedAt.Time
	}

	return step, nil
}

func updateRunTx(ctx context.Context, tx *sql.Tx, run Run) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE workflow_runs
		SET workflow_id = $3,
		    status = $4,
		    trigger_mode = $5,
		    attempt = $6,
		    max_attempts = $7,
		    input_json = $8,
		    output_json = $9,
		    error_message = $10,
		    created_at = $11,
		    started_at = $12,
		    completed_at = $13
		WHERE id = $1 AND tenant_id = $2
	`, run.ID, run.TenantID, run.WorkflowID, run.Status, run.TriggerMode, run.Attempt, run.MaxAttempts,
		mustJSON(run.Input), nullableJSON(run.Output), run.Error, run.CreatedAt, run.StartedAt, nullableTime(run.CompletedAt))
	if err != nil {
		return fmt.Errorf("update workflow run in transaction: %w", err)
	}

	return nil
}

func replaceStepsTx(ctx context.Context, tx *sql.Tx, runID string, steps []Step) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_steps WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("delete workflow steps: %w", err)
	}

	for _, step := range steps {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO workflow_steps (
				id, run_id, node_id, node_type, status, attempt,
				input_json, output_json, error_message, started_at, completed_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, step.ID, step.RunID, step.NodeID, step.NodeType, step.Status, step.Attempt,
			mustJSON(step.Input), nullableJSON(step.Output), step.Error, step.StartedAt, nullableTime(step.CompletedAt))
		if err != nil {
			return fmt.Errorf("insert workflow step: %w", err)
		}
	}

	return nil
}

func mustJSON(value any) []byte {
	if value == nil {
		return []byte("{}")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return encoded
}

func nullableJSON(value any) any {
	if value == nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return encoded
}

func decodeJSON(raw []byte, target any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, target)
}

func decodeNullableJSON(raw []byte, target *any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, target)
}

func nullableTime(value sqlTime) any {
	if value.IsZero() {
		return nil
	}
	return value
}

type sqlTime interface {
	IsZero() bool
}
