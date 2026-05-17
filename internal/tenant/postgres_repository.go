package tenant

import (
	"context"
	"database/sql"
	"fmt"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) GetByID(ctx context.Context, id string) (Tenant, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, name, status
		FROM tenants
		WHERE id = $1
	`, id)

	var tenant Tenant
	if err := row.Scan(&tenant.ID, &tenant.Name, &tenant.Status); err != nil {
		if err == sql.ErrNoRows {
			return Tenant{}, ErrNotFound
		}
		return Tenant{}, fmt.Errorf("query tenant by id: %w", err)
	}

	return tenant, nil
}

func (r *PostgresRepository) Upsert(ctx context.Context, tenant Tenant) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO tenants (id, name, status)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE
		SET name = EXCLUDED.name,
		    status = EXCLUDED.status
	`, tenant.ID, tenant.Name, tenant.Status)
	if err != nil {
		return fmt.Errorf("upsert tenant: %w", err)
	}

	return nil
}
