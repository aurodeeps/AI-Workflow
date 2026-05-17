package audit

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

func (r *PostgresRepository) Create(ctx context.Context, event Event) error {
	metadataJSON, err := json.Marshal(cloneMap(event.Metadata))
	if err != nil {
		return fmt.Errorf("encode audit event metadata: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO audit_events (
			id, tenant_id, run_id, workflow_id, step_id, node_id,
			type, message, metadata_json, created_at
		)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7, $8, $9, $10)
	`, event.ID, event.TenantID, event.RunID, event.WorkflowID, event.StepID, event.NodeID,
		event.Type, event.Message, metadataJSON, event.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}

	return nil
}

func (r *PostgresRepository) ListByRun(ctx context.Context, tenantID, runID string) ([]Event, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, run_id, workflow_id, COALESCE(step_id, ''), COALESCE(node_id, ''),
		       type, message, metadata_json, created_at
		FROM audit_events
		WHERE tenant_id = $1 AND run_id = $2
		ORDER BY created_at ASC, id ASC
	`, tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("query audit events by run: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var (
			event        Event
			metadataJSON []byte
		)
		if err := rows.Scan(
			&event.ID,
			&event.TenantID,
			&event.RunID,
			&event.WorkflowID,
			&event.StepID,
			&event.NodeID,
			&event.Type,
			&event.Message,
			&metadataJSON,
			&event.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
			if err := json.Unmarshal(metadataJSON, &event.Metadata); err != nil {
				return nil, fmt.Errorf("decode audit event metadata: %w", err)
			}
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}

	return events, nil
}
