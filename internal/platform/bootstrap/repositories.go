package bootstrap

import (
	"context"
	"database/sql"
	"fmt"

	"workflow/internal/audit"
	"workflow/internal/auth"
	"workflow/internal/documents"
	"workflow/internal/executor"
	"workflow/internal/platform/config"
	"workflow/internal/platform/persistence"
	"workflow/internal/tenant"
	"workflow/internal/workflow"
)

type Repositories struct {
	Tenants      tenant.Repository
	Auth         auth.Repository
	Documents    documents.Repository
	Audit        audit.Repository
	Workflows    workflow.Repository
	Executor     executor.Repository
	DB           *sql.DB
	UsePostgres  bool
	Close        func() error
}

func OpenRepositories(ctx context.Context, cfg config.Config) (Repositories, error) {
	if cfg.Dependencies.PostgresDSN == "" {
		tenantRepo := tenant.NewMemoryRepository(buildBootstrapTenants(cfg.Auth.BootstrapAPIKeys))
		return Repositories{
			Tenants:     tenantRepo,
			Auth:        auth.NewMemoryRepository(buildBootstrapAPIKeys(cfg.Auth.BootstrapAPIKeys)),
			Documents:   documents.NewMemoryRepository(),
			Audit:       audit.NewMemoryRepository(),
			Workflows:   workflow.NewMemoryRepository(),
			Executor:    executor.NewMemoryRepository(),
			UsePostgres: false,
			Close:       func() error { return nil },
		}, nil
	}

	db, err := persistence.OpenPostgres(ctx, cfg.Dependencies.PostgresDSN)
	if err != nil {
		return Repositories{}, err
	}

	tenantRepo := tenant.NewPostgresRepository(db)
	authRepo := auth.NewPostgresRepository(db)
	documentRepo := documents.NewPostgresRepository(db)
	auditRepo := audit.NewPostgresRepository(db)
	workflowRepo := workflow.NewPostgresRepository(db)
	executorRepo := executor.NewPostgresRepository(db)

	if err := seedBootstrapData(ctx, tenantRepo, authRepo, cfg.Auth.BootstrapAPIKeys); err != nil {
		_ = db.Close()
		return Repositories{}, err
	}

	return Repositories{
		Tenants:     tenantRepo,
		Auth:        authRepo,
		Documents:   documentRepo,
		Audit:       auditRepo,
		Workflows:   workflowRepo,
		Executor:    executorRepo,
		DB:          db,
		UsePostgres: true,
		Close:       db.Close,
	}, nil
}

func seedBootstrapData(ctx context.Context, tenantRepo *tenant.PostgresRepository, authRepo *auth.PostgresRepository, entries []config.BootstrapAPIKey) error {
	for _, entry := range entries {
		if err := tenantRepo.Upsert(ctx, tenant.Tenant{
			ID:     entry.TenantID,
			Name:   entry.TenantName,
			Status: tenant.StatusActive,
		}); err != nil {
			return fmt.Errorf("upsert bootstrap tenant %s: %w", entry.TenantID, err)
		}

		if err := authRepo.Upsert(ctx, auth.Credential{
			ID:       entry.APIKeyID,
			TenantID: entry.TenantID,
			Label:    entry.Label,
			KeyHash:  auth.HashAPIKey(entry.PlaintextKey),
			Status:   auth.StatusActive,
		}); err != nil {
			return fmt.Errorf("upsert bootstrap api key %s: %w", entry.APIKeyID, err)
		}
	}

	return nil
}

func buildBootstrapTenants(entries []config.BootstrapAPIKey) []tenant.Tenant {
	seen := make(map[string]struct{}, len(entries))
	tenants := make([]tenant.Tenant, 0, len(entries))

	for _, entry := range entries {
		if _, exists := seen[entry.TenantID]; exists {
			continue
		}

		seen[entry.TenantID] = struct{}{}
		tenants = append(tenants, tenant.Tenant{
			ID:     entry.TenantID,
			Name:   entry.TenantName,
			Status: tenant.StatusActive,
		})
	}

	return tenants
}

func buildBootstrapAPIKeys(entries []config.BootstrapAPIKey) []auth.APIKey {
	keys := make([]auth.APIKey, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, auth.APIKey{
			ID:        entry.APIKeyID,
			TenantID:  entry.TenantID,
			Label:     entry.Label,
			Plaintext: entry.PlaintextKey,
			Status:    auth.StatusActive,
		})
	}

	return keys
}
