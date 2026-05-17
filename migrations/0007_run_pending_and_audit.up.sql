CREATE TABLE workflow_run_pending (
    run_id TEXT PRIMARY KEY REFERENCES workflow_runs(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    waiting_node_id TEXT NOT NULL,
    queue_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    step_outputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    executed_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_workflow_run_pending_tenant_id ON workflow_run_pending (tenant_id);

CREATE TABLE audit_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_id TEXT,
    node_id TEXT,
    type TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    metadata_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_events_run_id_created_at ON audit_events (run_id, created_at);
CREATE INDEX idx_audit_events_tenant_id_created_at ON audit_events (tenant_id, created_at);
