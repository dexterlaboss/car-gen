FROM rust:1.72-slim-bullseye AS build

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    software-properties-common \
    cmake \
    build-essential \
    wget \
    libclang-dev \
    libudev-dev \
    libssl-dev \
    ca-certificates \
    perl \
    && apt-get update && apt-get install -y openjdk-11-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ENV HADOOP_VERSION=3.4.0
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# Set HADOOP_HOME and update PATH
ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="$HADOOP_HOME/bin:$PATH"

# Set JAVA_HOME and update LD_LIBRARY_PATH for Java libraries
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# ENV LD_LIBRARY_PATH="$JAVA_HOME/lib/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV LD_LIBRARY_PATH="$JAVA_HOME/lib/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV CLASSPATH $HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*

RUN cp $HADOOP_HOME/lib/native/libhdfs.so /usr/local/lib


RUN USER=root cargo new --bin solana
WORKDIR /solana

COPY . /solana

RUN cargo build --release



FROM rust:1.72-slim-bullseye

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
#    software-properties-common \
#    libssl-dev \
#    ca-certificates \
#    perl \
    wget \
    && apt-get update && apt-get install -y openjdk-11-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop
ENV HADOOP_VERSION=3.4.0
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# Set HADOOP_HOME and update PATH
ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="$HADOOP_HOME/bin:$PATH"

# Set JAVA_HOME and update LD_LIBRARY_PATH for Java libraries
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# ENV LD_LIBRARY_PATH="$JAVA_HOME/lib/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV LD_LIBRARY_PATH="$JAVA_HOME/lib/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV CLASSPATH $HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*
ENV CFLAGS="-I$JAVA_HOME/include -I$JAVA_HOME/include/linux"

RUN cp $HADOOP_HOME/lib/native/libhdfs.so /usr/local/lib

COPY --from=build /solana/docker/config/log4j.properties /opt/hadoop/etc/hadoop/log4j.properties

# Set working directory
WORKDIR /usr/local/bin

# Copy the built binary from host to container
#COPY target/release/ingestor-kafka-hbase .
COPY --from=build /solana/target/release/ingestor-kafka-hbase .
#COPY docker/config/.env.test ./.env

# Make the binary executable
RUN chmod +x ingestor-kafka-hbase

ENV RUST_LOG=info

# Set entrypoint
ENTRYPOINT ["./ingestor-kafka-hbase"]
