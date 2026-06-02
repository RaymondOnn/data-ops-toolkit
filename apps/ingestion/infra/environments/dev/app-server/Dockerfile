FROM python:3.11-slim

# 1. Install system dependencies and AWS CLI v2
RUN apt-get update && apt-get install -y \
    openssh-server \
    curl \
    unzip \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws \
    && rm -rf /var/lib/apt/lists/*

# 2. Setup SSH for deployment
RUN mkdir /var/run/sshd
RUN useradd -m -s /bin/bash deploy
RUN mkdir -p /home/deploy/.ssh && chmod 700 /home/deploy/.ssh
RUN mkdir -p /opt/deploy && chown -R deploy:deploy /opt/deploy

# This allows GitHub Actions to login
COPY deploy_key.pub /home/deploy/.ssh/authorized_keys
RUN chown -R deploy:deploy /home/deploy/.ssh && chmod 600 /home/deploy/.ssh/authorized_keys

# 3. Environment Configuration
# Global PEX_PATH ensures any PEX run here automatically finds its dependency layer
ENV PEX_PATH=/opt/deploy/current_deps.pex

# Pre-configure AWS CLI to point to LocalStack
RUN mkdir -p /home/deploy/.aws && \
    printf "[default]\naws_access_key_id = test\naws_secret_access_key = test\n" > /home/deploy/.aws/credentials && \
    printf "[default]\nregion = ap-southeast-1\noutput = json\n" > /home/deploy/.aws/config && \
    chown -R deploy:deploy /home/deploy/.aws

WORKDIR /opt/deploy

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
