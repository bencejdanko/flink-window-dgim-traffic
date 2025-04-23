# At the beginning of your Dockerfile
ARG USER_UID=1001
ARG USER_GID=1001
ARG USER_NAME=flink_user

FROM flink:1.20.1-scala_2.12-java17

# === User/Group Setup (Combine with previous fix if you added it) ===
# Run as root initially to perform setup
USER root

# Install dependencies (keep your existing RUN command)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip sudo && \
    rm -rf /var/lib/apt/lists/*

# Create the group and user if they don't exist
# Use --non-unique flags in case the GID/UID already exists (e.g., for root group if GID=0)
RUN groupadd --gid $USER_GID --non-unique $USER_NAME || echo "Group $USER_GID already exists" && \
    useradd --uid $USER_UID --gid $USER_GID --non-unique --create-home --shell /bin/bash $USER_NAME || echo "User $USER_UID already exists"
# Optionally add to sudo if needed for debugging
# RUN adduser $USER_NAME sudo \
#    && echo "$USER_NAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# === Fix Flink Directory Permissions ===
RUN chmod -R o+w /opt/flink/log /opt/flink/conf /opt/flink/plugins


# === Continue with your setup ===
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install pip packages as the target user to avoid permission issues in site-packages later
# Note: If pip needs root, you might need to do this before the chown/user switch or use sudo
# If installing globally, root is fine, but user-specific might be better if needed.
# Let's assume global install is ok for now.
# RUN pip install --no-cache-dir apache-flink==2.0.0

# Download connector JARs (keep your existing RUN command)
RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar

# Optional: Switch to the user if desired, though docker-compose 'user:' directive handles runtime
# USER $USER_UID:$USER_GID
# or
# USER $USER_NAME

# WORKDIR /opt/flink # Setting WORKDIR is generally good practice

# The base image's ENTRYPOINT/CMD will likely handle starting Flink processes

COPY ./src/requirements.txt /opt/flink/requirements.txt
WORKDIR /opt/flink
RUN pip install --no-cache-dir -r requirements.txt