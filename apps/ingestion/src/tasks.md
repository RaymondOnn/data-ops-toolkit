
ZombieState.is_applicable harding and make shift recovery into ZombieState
Split Task into smaller components i.e TaskState
incorporate data retention policies i.e. can keep data for 1 year
disable self-healing
masking type
error_handling / exception hook /

1. Artifact Verification (The "MD5 Handshake")
Currently, you upload to S3 and then pull to the server. If a network hiccup occurs during the upload, you might end up with a corrupted PEX on your server.

The Improvement: Generate an MD5 checksum of the PEX on your Mac runner and save it as an S3 object (e.g., app.pex.md5).

Why: After the App Server pulls the PEX, it should run md5sum -c app.pex.md5. If they don't match, the deployment should abort immediately before switching the symlink. This prevents running "broken" binaries.

1. The "Pre-Switch" Validation (Dry Run)
Your health check currently happens after the symlink is switched. If the new version is broken, the symlink is already pointing to it.

The Improvement: Run the health check on the new folder before running the ln -sfn command.

Bash

# Inside the deployment script

/opt/deploy/ingestion/$RELEASE_ID/app.pex --version || exit 1

# ONLY IF ABOVE PASSES, then switch the link

ln -sfn /opt/deploy/ingestion/$RELEASE_ID/app.pex /opt/deploy/current_app.pex
Why: This turns your deployment into a "Blue-Green" style switch at the directory level, ensuring the "Current" pointer only ever points to something that can actually boot up.

1. Cleanup Strategy: S3 vs. Server
You have a cleanup job on the server (keeping last 5 releases), but does your S3 bucket also get cleaned up?

The Improvement: Add a Lifecycle Policy to your LocalStack S3 bucket or a step in your cd.yaml to delete old versions in S3.

Why: In a real cloud environment, storing every PEX version forever gets expensive. Matching the retention (5 versions) between S3 and the Server keeps your "Simulated Cloud" synchronized.

1. PEX Layering Strategy (The "Cache" Check)
In cd.yaml, you are building deps.pex on every single push. Since deps.pex contains your heavy libraries (Polars, etc.), it takes a long time to build and move.

The Improvement: Only build and upload deps.pex if pyproject.toml or requirements.txt has changed.

Why: In a real CI/CD pipeline, this can shave 5–10 minutes off every build. You can use a hashFiles('/pyproject.toml') check in GitHub Actions to see if you can skip the dependency build and just pull the "last known good" deps.pex from S3.

1. Multi-Container Deployment (Parallelism)
Your current script handles one app server. If you have both a DEV and a PROD container running:

The Improvement: Use the GitHub environment feature to map the SSH ports dynamically.

YAML

# Example snippet

host: ${{ secrets.SSH_HOST }}
port: ${{ github.event.inputs.environment == 'prod' && 2223 || 2222 }}
Why: This allows the same workflow to target different containers based on your input, proving you can manage multi-stage environments from a single pipeline.
