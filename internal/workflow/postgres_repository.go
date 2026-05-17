package workflow

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

func (r *PostgresRepository) Create(ctx context.Context, workflow Workflow) error {
	definitionJSON, err := json.Marshal(workflow.Definition)
	if err != nil {
		return fmt.Errorf("encode workflow definition: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO workflows (
			id, tenant_id, name, description, version, status,
			validation_status, definition_json, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, workflow.ID, workflow.TenantID, workflow.Name, workflow.Description, workflow.Version,
		workflow.Status, workflow.ValidationStatus, definitionJSON, workflow.CreatedAt, workflow.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert workflow: %w", err)
	}

	return nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, tenantID, workflowID string) (Workflow, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, description, version, status,
		       validation_status, definition_json, created_at, updated_at
		FROM workflows
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, workflowID)

	var (
		resolved       Workflow
		definitionJSON []byte
	)
	if err := row.Scan(
		&resolved.ID,
		&resolved.TenantID,
		&resolved.Name,
		&resolved.Description,
		&resolved.Version,
		&resolved.Status,
		&resolved.ValidationStatus,
		&definitionJSON,
		&resolved.CreatedAt,
		&resolved.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return Workflow{}, ErrNotFound
		}
		return Workflow{}, fmt.Errorf("query workflow by id: %w", err)
	}
	if err := json.Unmarshal(definitionJSON, &resolved.Definition); err != nil {
		return Workflow{}, fmt.Errorf("decode workflow definition: %w", err)
	}

	return resolved, nil
}
