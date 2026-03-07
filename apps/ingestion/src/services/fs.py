

@ServiceFactory.register("s3")
class S3Service(Service):
    def stage_data(self, lf: pl.LazyFrame, context: WriteContext) -> StagingResult:
        staging_path = f"s3://{self.config['bucket']}/tmp/{context.target}/"
        self.client.upload_parquet(lf, staging_path)
        return StagingResult(staging_path=staging_path, rows=0) # Update with actual count if needed

    def promote_data(self, result: StagingResult, context: WriteContext):
        # Partitioned Path: table/year=2024/month=03/
        final_path = f"s3://{self.config['bucket']}/{context.target}/{context.partition_col}={context.partition_value}/"
        self.client.move_objects(result.staging_path, final_path)