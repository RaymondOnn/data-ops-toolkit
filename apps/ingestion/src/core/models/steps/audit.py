



class AuditStep(JobStep):
    manifest: AuditPayload

    def execute(self, job: "Job") -> str:
        start_time = time.perf_counter()

        try:
            write_meta: AuditPayload = (
                manifest.write
            )  # Access staging info from WriteStep

            # 1. Internal Heuristic Checks (The 'Stand-in' Logic)
            # While the external app is missing, we check basic things:
            # - Did we lose more than 10% of data?
            # - Are there nulls in the Primary Key?
            internal_results = self._run_internal_checks(job, write_meta)

            # 2. External App Mock Hook
            # This is where you will eventually call your validation API
            external_status = "NOT_AVAILABLE"

            # # 1. Construct the Shell Command
            # # We pass the staging artifact (table/path) as an argument to the app
            # cmd = [
            #     "validation-app",
            #     "--source", write_meta.staging_artifact,
            #     "--job-id", job.id,
            #     "--run-id", job.run_id
            # ]

            # # 2. Run the Command
            # # capture_output=True allows us to save the logs into our manifest
            # process = subprocess.run(
            #     cmd,
            #     capture_output=True,
            #     text=True,
            #     check=False  # We handle the error manually to finalize the manifest
            # )

            # 3. Build Payload
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            payload = AuditPayload(
                step_outcome="COMPLETED",
                validation_passed=internal_results["passed"],
                total_checks_run=len(internal_results["checks"]),
                failed_checks=internal_results["failures"],
                external_app_status=external_status,
                audit_duration_ms=duration_ms,
            )

            self.finalize(job, results=payload)

            # If validation fails, we stop the pipeline here!
            if not payload.validation_passed:
                raise ValueError(
                    f"Audit failed for Job {job.id}. See manifest for details."
                )

            return self._transit(job)

        except Exception as e:
            self.finalize(job, exception=e)
            raise

    def _run_internal_checks(
        self, job: "Job", write_meta: AuditPayload
    ) -> dict[str, Any]:
        """Simple baseline checks while the real app is under construction."""
        # Example: Check if rows_affected is 0
        checks = []
        failures = []

        # Check 1: Row count > 0
        checks.append("row_count_not_zero")
        if write_meta.rows_affected == 0:
            failures.append(
                {
                    "check": "row_count_not_zero",
                    "message": "No rows were loaded to staging.",
                }
            )

        return {"passed": len(failures) == 0, "checks": checks, "failures": failures}
