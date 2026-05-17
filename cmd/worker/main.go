package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"workflow/internal/audit"
	"workflow/internal/documents"
	"workflow/internal/executor"
	appbootstrap "workflow/internal/platform/bootstrap"
	"workflow/internal/platform/config"
	"workflow/internal/platform/logging"
	"workflow/internal/platform/observability"
	"workflow/internal/tenant"
	appworker "workflow/internal/worker"
	"workflow/internal/workflow"
)

func main() {
	cfg, err := config.Load("workflow-worker")
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
	if cfg.Dependencies.RedisAddr == "" {
		logger.Error("standalone worker requires REDIS_ADDR to be configured")
		os.Exit(1)
	}

	repositories, err := appbootstrap.OpenRepositories(context.Background(), cfg)
	if err != nil {
		logger.Error("failed to initialize repositories", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := repositories.Close(); err != nil {
			logger.Error("failed to close repositories", "error", err)
		}
	}()

	objectStore, err := buildObjectStore(cfg)
	if err != nil {
		logger.Error("failed to initialize object storage", "error", err)
		os.Exit(1)
	}
	documentService := documents.NewService(repositories.Documents, objectStore)
	workflowService := workflow.NewService(repositories.Workflows)
	auditService := audit.NewService(repositories.Audit)
	triggerControl := tenant.NewTriggerControl(cfg.Limits.TenantExecuteLimit, cfg.Limits.TenantExecuteWindow)
	queue := appworker.NewRedisQueue(cfg.Dependencies.RedisAddr, "")
	executorService := executor.NewService(
		repositories.Executor,
		workflowService,
		documentService,
		executor.NewMockLLMProvider(),
	).WithJobQueue(queue).WithAudit(auditService).WithTriggerControl(triggerControl)
	workerService := appworker.NewService(logger, queue, executorService)

	logger.Info("worker starting", "redis_addr", cfg.Dependencies.RedisAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := workerService.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker stopped unexpectedly", "error", err)
		os.Exit(1)
	}
	logger.Info("worker stopped")
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
