package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"workflow/internal/api"
	"workflow/internal/audit"
	"workflow/internal/auth"
	"workflow/internal/documents"
	"workflow/internal/executor"
	appbootstrap "workflow/internal/platform/bootstrap"
	"workflow/internal/platform/config"
	"workflow/internal/platform/health"
	"workflow/internal/platform/logging"
	"workflow/internal/platform/observability"
	"workflow/internal/tenant"
	appworker "workflow/internal/worker"
	"workflow/internal/workflow"
)

func main() {
	cfg, err := config.Load("workflow-api")
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName, cfg.AppEnv)
	shutdownObservability, err := observability.Setup(context.Background(), cfg)
	if err != nil {
		logger.Error("failed to initialize observability", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := shutdownObservability(context.Background()); err != nil {
			logger.Error("failed to shut down observability", "error", err)
		}
	}()
	healthService := health.NewService(
		cfg.ServiceName,
		cfg.Dependencies.ReadinessTimeout,
		buildDependencyCheckers(cfg)...,
	)
	bootstrapCtx := context.Background()
	repositories, err := appbootstrap.OpenRepositories(bootstrapCtx, cfg)
	if err != nil {
		logger.Error("failed to initialize repositories", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := repositories.Close(); err != nil {
			logger.Error("failed to close repositories", "error", err)
		}
	}()

	tenantService := tenant.NewService(repositories.Tenants)
	authService := auth.NewService(repositories.Auth, tenantService)
	triggerControl := tenant.NewTriggerControl(cfg.Limits.TenantExecuteLimit, cfg.Limits.TenantExecuteWindow)
	auditService := audit.NewService(repositories.Audit)
	objectStore, err := buildObjectStore(cfg)
	if err != nil {
		logger.Error("failed to initialize object storage", "error", err)
		os.Exit(1)
	}
	documentService := documents.NewService(
		repositories.Documents,
		objectStore,
	)
	workflowService := workflow.NewService(repositories.Workflows)
	jobQueue := buildJobQueue(cfg)
	executorService := executor.NewService(
		repositories.Executor,
		workflowService,
		documentService,
		executor.NewMockLLMProvider(),
	).WithJobQueue(jobQueue).WithAudit(auditService).WithTriggerControl(triggerControl)
	backgroundWorker := appworker.NewService(logger, jobQueue, executorService)

	server := &http.Server{
		Addr:         cfg.HTTP.Addr,
		Handler:      api.NewHandler(logger, healthService, authService, documentService, workflowService, executorService, auditService),
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	if cfg.Dependencies.RedisAddr == "" {
		go func() {
			if err := backgroundWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}
	go func() {
		logger.Info("api server starting", "addr", cfg.HTTP.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		logger.Error("api server stopped unexpectedly", "error", err)
		os.Exit(1)
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down api server cleanly", "error", err)
		os.Exit(1)
	}

	if err := shutdownObservability(shutdownCtx); err != nil {
		logger.Error("failed to flush observability", "error", err)
		os.Exit(1)
	}
	shutdownObservability = func(context.Context) error { return nil }

	logger.Info("api server stopped")
}

func buildDependencyCheckers(cfg config.Config) []health.Checker {
	checkers := make([]health.Checker, 0, 3)

	if cfg.Dependencies.PostgresAddr != "" {
		checkers = append(checkers, health.NewTCPChecker("postgres", cfg.Dependencies.PostgresAddr))
	}

	if cfg.Dependencies.RedisAddr != "" {
		checkers = append(checkers, health.NewTCPChecker("redis", cfg.Dependencies.RedisAddr))
	}

	if cfg.Dependencies.MinIOAddr != "" {
		checkers = append(checkers, health.NewTCPChecker("minio", cfg.Dependencies.MinIOAddr))
	}

	return checkers
}

func buildJobQueue(cfg config.Config) executor.JobQueue {
	if cfg.Dependencies.RedisAddr != "" {
		return appworker.NewRedisQueue(cfg.Dependencies.RedisAddr, "")
	}

	return appworker.NewMemoryQueue(128)
}

func buildObjectStore(cfg config.Config) (documents.ObjectStore, error) {
	if cfg.Storage.S3.Endpoint == "" {
		return documents.NewLocalObjectStore(cfg.Storage.ObjectDir), nil
	}

	return documents.NewS3ObjectStore(
		cfg.Storage.S3.Endpoint,
		cfg.Storage.S3.AccessKey,
		cfg.Storage.S3.SecretKey,
		cfg.Storage.S3.Bucket,
		cfg.Storage.S3.UseSSL,
	)
}
