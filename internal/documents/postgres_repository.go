package documents

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

func (r *PostgresRepository) Create(ctx context.Context, document Document, chunks []Chunk) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin document transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO documents (
			id, tenant_id, object_key, filename, content_type, size_bytes,
			checksum, status, created_at, ingested_at, chunk_count
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, document.ID, document.TenantID, document.ObjectKey, document.Filename, document.ContentType, document.SizeBytes,
		document.Checksum, document.Status, document.CreatedAt, nullableTime(document.IngestedAt), document.ChunkCount)
	if err != nil {
		return fmt.Errorf("insert document: %w", err)
	}

	for _, chunk := range chunks {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO document_chunks (
				id, tenant_id, document_id, chunk_index, content, token_estimate, created_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, chunk.ID, chunk.TenantID, chunk.DocumentID, chunk.ChunkIndex, chunk.Content, chunk.TokenEstimate, chunk.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert document chunk: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit document transaction: %w", err)
	}

	return nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, tenantID, documentID string) (Document, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, filename, content_type, size_bytes, checksum,
		       object_key, status, created_at, ingested_at, chunk_count
		FROM documents
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, documentID)

	var (
		document   Document
		ingestedAt sql.NullTime
	)
	if err := row.Scan(
		&document.ID,
		&document.TenantID,
		&document.Filename,
		&document.ContentType,
		&document.SizeBytes,
		&document.Checksum,
		&document.ObjectKey,
		&document.Status,
		&document.CreatedAt,
		&ingestedAt,
		&document.ChunkCount,
	); err != nil {
		if err == sql.ErrNoRows {
			return Document{}, ErrNotFound
		}
		return Document{}, fmt.Errorf("query document by id: %w", err)
	}
	if ingestedAt.Valid {
		document.IngestedAt = ingestedAt.Time
	}

	return document, nil
}

func (r *PostgresRepository) ListChunks(ctx context.Context, tenantID, documentID string) ([]Chunk, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT c.id, c.document_id, c.tenant_id, c.chunk_index, c.content, c.token_estimate, c.created_at
		FROM document_chunks c
		INNER JOIN documents d ON d.id = c.document_id
		WHERE c.tenant_id = $1 AND c.document_id = $2 AND d.tenant_id = $1
		ORDER BY c.chunk_index ASC
	`, tenantID, documentID)
	if err != nil {
		return nil, fmt.Errorf("query document chunks: %w", err)
	}
	defer rows.Close()

	chunks := make([]Chunk, 0)
	for rows.Next() {
		var chunk Chunk
		if err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.TenantID,
			&chunk.ChunkIndex,
			&chunk.Content,
			&chunk.TokenEstimate,
			&chunk.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan document chunk: %w", err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate document chunks: %w", err)
	}

	if len(chunks) == 0 {
		if _, err := r.GetByID(ctx, tenantID, documentID); err != nil {
			return nil, err
		}
	}

	return chunks, nil
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
