FROM python:3.11-slim

# 1. Install system dependencies
RUN apt-get update && apt-get install -y \
    openssh-server \
    awscli \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Setup SSH for deployment
RUN mkdir /var/run/sshd
RUN useradd -m -s /bin/bash deploy
RUN mkdir -p /home/deploy/.ssh && chmod 700 /home/deploy/.ssh

# This allows GitHub Actions to login
COPY deploy_key.pub /home/deploy/.ssh/authorized_keys
RUN chown -R deploy:deploy /home/deploy/.ssh && chmod 600 /home/deploy/.ssh/authorized_keys

# 3. Pre-configure AWS CLI to point to LocalStack
# We use dummy credentials because LocalStack doesn't validate them
RUN mkdir -p /home/deploy/.aws
RUN echo "[default]\naws_access_key_id = test\naws_secret_access_key = test" > /home/deploy/.aws/credentials
RUN echo "[default]\nregion = us-east-1\noutput = json" > /home/deploy/.aws/config
RUN chown -R deploy:deploy /home/deploy/.aws

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]