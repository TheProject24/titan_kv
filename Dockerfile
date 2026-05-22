# --- Stage 1: The Compiler ---
    FROM rust:1.81-slim-bookworm AS builder
    
    WORKDIR /usr/src/titan_kv
    
    # Copy your source tree and configuration files
    COPY Cargo.toml ./
    COPY src ./src
    
    # Compile the binary in release mode for maximum performance optimization
    RUN cargo build --release
    
    # --- Stage 2: The Ultra-Light Production Runtime ---
    FROM debian:bookworm-slim
    
    WORKDIR /app
    
    # Copy only the compiled binary from the builder stage
    COPY --from=builder /usr/src/titan_kv/target/release/titan_kv .
    
    # Expose the standard Redis port that Titan KV listens on
    EXPOSE 6379
    
    # Launch your engine
    CMD ["./titan_kv"]