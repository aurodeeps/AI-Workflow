package auth

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

func (r *PostgresRepository) LookupByHash(ctx context.Context, keyHash string) (Credential, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, label, key_hash, status
		FROM api_keys
		WHERE key_hash = $1
	`, keyHash)

	var credential Credential
	if err := row.Scan(&credential.ID, &credential.TenantID, &credential.Label, &credential.KeyHash, &credential.Status); err != nil {
		if err == sql.ErrNoRows {
			return Credential{}, ErrInvalidAPIKey
		}
		return Credential{}, fmt.Errorf("query api key by hash: %w", err)
	}

	return credential, nil
}

func (r *PostgresRepository) Upsert(ctx context.Context, credential Credential) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO api_keys (id, tenant_id, key_hash, label, status)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE
		SET tenant_id = EXCLUDED.tenant_id,
		    key_hash = EXCLUDED.key_hash,
		    label = EXCLUDED.label,
		    status = EXCLUDED.status
	`, credential.ID, credential.TenantID, credential.KeyHash, credential.Label, credential.Status)
	if err != nil {
		return fmt.Errorf("upsert api key: %w", err)
	}

	return nil
}
