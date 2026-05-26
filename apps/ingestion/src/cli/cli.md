# CLI Commands

## 4. resume | recover (Resilience)

Attempts to finish failed or interrupted jobs.

### Subcommands

- `resume <run_id>`: Restarts from the exact point of failure.
- `resume --failed`: Automatically identifies all failed runs in the last 24h and attempts a retry.

1. How can I have resume command work for both execution modes?
2. Maybe the positional arguments can allowed for multiple values. For e.g. when there's a outage for a particular source? Any way for a argument to be flexible like a where clause?
3. I'll thinking

### Args/Options

- `--rewind <stage>`: Force the task to restart from a specific stage (e.g., transform), even if it failed at archive.

## 5. test & dev (Validation)

Splitting these is wise: Test is for validation; Dev is for local iteration.

### test (The Auditor)

- `test config <file>`: Validates YAML schema and Pydantic/Msgspec models.
- `test logic <job_name>`: Runs unit tests against the custom transformation logic.

### dev (The Iterative Sandbox)

- `dev sandbox <job_name>`: Runs a full ingestion but points all outputs to a `.workspace/dev/` folder instead of production S3.
- `dev mock <stage>`: Runs a stage with "mocked" data to verify pipeline connectivity.

## 8. cmd (Runtime Interaction)

This allows real-time communication with the serve process.

### Subcommands

- `cmd pause`: Pauses the orchestrator from picking up new tasks.
- `cmd log-level <level>`: Dynamically changes the logger level (DEBUG/INFO) without restarting.
- `cmd reload`: Forces the orchestrator to refresh job configurations from disk/DB.

## Summary Table for CLI UX

| Command | Primary Use Case       | Primary Subcommand     |
|---------|------------------------|------------------------|
| run     | Ad-hoc / CI-CD trigger | `run local`            |
| serve   | Production Service     | `serve --mode reactive`|
| stop    | Maintenance            | `stop --drain`         |
| resume  | Incident Recovery      | `resume --failed`      |
| test    | Quality Assurance      | `test config`          |
| dev     | Local Prototyping      | `dev sandbox`          |
| clean   | Space Management       | `clean --expired`      |
| doctor  | Troubleshooting        | `doctor s3`            |
| cmd     | Live Tuning            | `cmd reload`           |

app
├── start
├── stop
├── run [DATE]
├── add [DATE]
├── resume
├── clean
├── status
├── doctor
│   ├── fs
│   ├── config
│   ├── net
│   └── connect
├── test
│   ├── cmp <a> <b>      # Compare datasets
│   ├── reg <name>       # Regression test (composite)
│   └── scenario <name>  # Stress test suite
└── dev
    └── clone <tr>
