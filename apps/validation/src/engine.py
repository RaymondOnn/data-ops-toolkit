import uuid
from pathlib import Path


import structlog

from apps.validation.src.core.models.dataset import Dataset
from apps.validation.src.core.models.validation.enums import ExecutionStatus, ValidationOutcome
from apps.validation.src.core.analyzer import Analyzer

# Initialize the structured logger
log = structlog.get_logger()

class Engine:
    def __init__(self, audit_db):
        self.audit_db = audit_db

    def execute(self, source: Dataset, sink: Dataset, config: dict):
        job_id = str(uuid.uuid4())
        job_log = log.bind(src=source.name, snk=sink.name, job_id=job_id)
        
        # 1. Initialize Run in DB
        self.audit_db.initialize_run(job_id, source.name, sink.name)
        job_log.info("validation_started", status=ExecutionStatus.RUNNING)

        try:
            # --- TIER 0: Schema Check ---
            job_log.info("executing_tier_0_schema_check")
            source_schema = source.get_schema()
            sink_schema = sink.get_schema()
            
            if not self._schemas_are_compatible(source_schema, sink_schema):
                job_log.error("schema_mismatch_detected")
                self._finalize_failure(job_id, "Schema Mismatch", job_log)
                return

            # --- TIERS 1-4: Mega Scan (Aggregate Metrics) ---
            # Single-pass streaming collect for counts, sums, and global hash
            job_log.info("executing_aggregate_scan")
            metrics = self._run_mega_scan(source, sink, source_schema)
            
            # --- TIER 5: Forensic Analysis (Conditional) ---
            forensic_results = None
            if metrics['src_hash'] != metrics['snk_hash']:
                job_log.warn("integrity_failure_detected", action="starting_forensics")
                
                analyzer = Analyzer(source, sink)
                id_col = config.get("primary_key")
                
                # Pre-flight: Validate anchor stability if no PK provided
                if not id_col:
                    anchors = analyzer._get_anchor_columns(id_col)
                    analyzer._validate_anchor_integrity(anchors)
                
                # Identify exactly what is missing or mismatched
                forensic_results = analyzer.analyze(
                    id_column=id_col,
                    sample_limit=config.get("sample_limit", 10)
                )

            # 2. Finalize Database and Logs
            self._finalize_success(job_id, metrics, forensic_results, job_log)

        except Exception as e:
            job_log.error("validation_crash", error=str(e), exc_info=True)
            self.audit_db.update_run(
                job_id=job_id,
                status=ExecutionStatus.ERROR,
                outcome=ValidationOutcome.FAILED,
                error_msg=str(e)
            )

    def _run_mega_scan(self, source, sink, schema):
        """Executes dual-track validation on both datasets."""
        source_lf = source.get_raw_stream()
        sink_lf = sink.get_raw_stream()

        # Pass the lazy schema (metadata only) to build expressions
        src_exprs = profile_check.get_expressions(source_lf.schema, mapping_df, is_source=True)
        snk_exprs = profile_check.get_expressions(sink_lf.schema, mapping_df, is_source=False)

        # Collect only the 1-row statistical profile
        source_profile = source_lf.select(src_exprs).collect(streaming=True)
        sink_profile = sink_lf.select(snk_exprs).collect(streaming=True)
        
        return {
            "src_count": src_res["row_count"][0],
            "snk_count": snk_res["row_count"][0],
            "src_hash": src_res["integrity_signature"][0],
            "snk_hash": snk_res["integrity_signature"][0],
            "metrics": src_res.to_dicts()[0]
        }

    def _purge_runtime_artifacts(self, job_id: str, cache_dir: str = "./audit_cache"):
        """
        Aggressively removes temporary storage files and diskcache folders.
        Ensures the 512MB environment remains lean for the next run.
        """
        path = Path(cache_dir)
        if not path.exists():
            return

        # 1. Remove Parquet/DB files for this specific job
        # (Assuming dataset names contain the job_id or session prefix)
        for artifact in path.glob(f"*{job_id}*"):
            try:
                if artifact.is_file():
                    artifact.unlink()
                elif artifact.is_dir():
                    import shutil
                    shutil.rmtree(artifact)
            except Exception as e:
                log.error("artifact_cleanup_failed", file=str(artifact), error=str(e))
                
    def _finalize_success(self, job_id, metrics, forensics, job_log):
        """Logic to determine final outcome and update DB."""
        volume_match = metrics['src_count'] == metrics['snk_count']
        integrity_match = metrics['src_hash'] == metrics['snk_hash']
        
        # Determine Outcome Enum
        if volume_match and integrity_match:
            outcome = ValidationOutcome.PASSED
        elif volume_match and not integrity_match:
            outcome = ValidationOutcome.FAILED # Data is corrupted
        else:
            outcome = ValidationOutcome.WARN # Likely missing records but some passed

        self._purge_runtime_artifacts(job_id)
        
        # Update Database (3-Table approach)
        self.audit_db.update_run(
            job_id=job_id,
            status=ExecutionStatus.SUCCESS,
            outcome=outcome,
            metrics=metrics,
            forensics=forensics # List of row-pair samples for validation_forensics table
        )
        
        job_log.info("validation_complete", outcome=outcome)

    def _finalize_failure(self, job_id, reason, job_log):
        """
        Aborts the validation run due to an unexpected error.
        Logs the outcome as FAILED and updates the audit database.
        """
        self._purge_runtime_artifacts(job_id)
        self.audit_db.update_run(
            job_id=job_id,
            status=ExecutionStatus.FAILURE,
            outcome=ValidationOutcome.FAILED,
            error_msg=reason
        )
        job_log.info("validation_aborted", reason=reason)
        
        """
        UPDATE audit_log
            SET 
                status = :status,
                outcome = :outcome,
                end_time = :end_time,
                metrics_json = :metrics_json,
                duration_seconds = EXTRACT(EPOCH FROM (:end_time - start_time))
            WHERE job_id = :job_id;
        """