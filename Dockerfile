# Use a minimal Rust image for building
FROM rust:1.86-slim-bullseye AS build

# Install only necessary dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    build-essential \
    libudev-dev \
    libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only Cargo files first to leverage Docker cache
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release

# Copy the actual source code and build
COPY . .
RUN cargo build --release && strip target/release/car-gen

# Use a smaller runtime image
FROM debian:bullseye-slim

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libudev-dev \
    libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin

COPY --from=build /app/target/release/car-gen .

ENV RUST_LOG=info

ENTRYPOINT ["./car-gen"]