from datetime import date, datetime
from pathlib import Path

import polars as pl
import yaml
from faker import Faker


def generate_simulation_artifacts(
    job_id: str,
    dataset: str,
    config_root: Path,
    target_root: Path,
    rows: int = 1000,
    force: bool = False,
) -> bool:
    """
    Bootstraps a simulation job with realistic mock data using Faker.
    Creates configs, schemas, and source parquet files.
    """
    job_root = config_root / job_id
    if job_root.exists() and not force:
        return False

    job_root.mkdir(parents=True, exist_ok=True)

    # 1. Generate config.yaml
    config_path = job_root / "config.yaml"
    config_data = {
        "default": {
            "job": {
                "extract": {"service_ref": "file", "num_workers": 4},
                "load": {"service_ref": "clickhouse", "partition_on": "event_date"},
            },
            "datasets": {
                dataset: {
                    "extract": {
                        # Path is relative to the flat_file service root
                        # (usually project root)
                        "resource": f"{target_root.name}/{job_id}/{dataset}",
                        "schema_file": "schema.csv",
                    },
                    "load": {"destination": f"TEST.{dataset.upper()}"},
                }
            },
        }
    }
    with config_path.open("w") as f:
        yaml.dump(config_data, f, default_flow_style=False)

    # 2. Generate schema.csv
    schema_path = job_root / "schema.csv"
    schema_content = (
        "column_name,data_type,is_nullable,is_primary_key\n"
        "id,Int64,False,True\n"
        "event_date,Date,False,False\n"
        "user_name,String,True,False\n"
        "email,String,True,False\n"
        "amount,Float64,True,False\n"
        "status,String,True,False\n"
        "updated_at,DateTime,False,False"
    )
    schema_path.write_text(schema_content)

    # 3. Generate Synthetic Data
    fake = Faker()
    data = {
        "id": range(rows),
        "event_date": [date(2024, 1, 1)] * rows,
        "user_name": [fake.name() for _ in range(rows)],
        "email": [fake.email() for _ in range(rows)],
        "amount": [
            round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2)
            for _ in range(rows)
        ],
        "status": [
            fake.random_element(elements=("active", "pending", "closed"))
            for _ in range(rows)
        ],
        "updated_at": [datetime(2024, 1, 1, 12, 0, 0)] * rows,
    }
    df = pl.DataFrame(data)

    # 4. Storage Provisioning
    source_dir = target_root / job_id / dataset
    source_dir.mkdir(parents=True, exist_ok=True)

    part_size = max(1, rows // 4)
    for i, start in enumerate(range(0, rows, part_size)):
        subset = df.slice(start, part_size)
        subset.write_parquet(
            source_dir / f"part_{i:03d}.parquet",
            compression="zstd",
            compression_level=3,
        )

    return True
