-- Dataset metadata tracking
CREATE TABLE datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    layer VARCHAR(50) NOT NULL CHECK (layer IN ('raw', 'processed', 'curated')),
    file_path TEXT NOT NULL,
    schema_json JSONB,
    row_count BIGINT,
    size_bytes BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    UNIQUE(name, version, layer)
);

CREATE INDEX idx_datasets_name ON datasets(name);
CREATE INDEX idx_datasets_layer ON datasets(layer);

-- Job queue and status
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    dataset_id INTEGER REFERENCES datasets(id),
    priority INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'preempted')),
    
    -- Resource requirements
    cpu_cores INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    gpu_count INTEGER DEFAULT 0,
    
    -- Execution details
    worker_id VARCHAR(100),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    checkpoint_path TEXT,
    progress FLOAT DEFAULT 0.0,
    
    -- Retry and scheduling
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Job configuration
    config JSONB
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_priority ON jobs(priority DESC);
CREATE INDEX idx_jobs_created ON jobs(created_at);

-- Resource allocation tracking
CREATE TABLE resource_allocations (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id VARCHAR(100) NOT NULL,
    cpu_cores INTEGER,
    memory_mb INTEGER,
    gpu_count INTEGER,
    allocated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    released_at TIMESTAMP
);

CREATE INDEX idx_allocations_job ON resource_allocations(job_id);
CREATE INDEX idx_allocations_worker ON resource_allocations(worker_id);

-- Job execution logs
CREATE TABLE job_logs (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(20),
    message TEXT
);

CREATE INDEX idx_logs_job ON job_logs(job_id);

-- Metrics tracking
CREATE TABLE job_metrics (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name VARCHAR(100),
    metric_value FLOAT,
    epoch INTEGER
);

CREATE INDEX idx_metrics_job ON job_metrics(job_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_jobs_updated_at BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();