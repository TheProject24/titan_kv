// src/config.rs
//! Titan KV Configuration System
//! 
//! This module uses 'clap' to parse CLI arguments and 'dotenv' style 
//! environment variables into a strongly-typed Config struct.

use clap::Parser;

/// Global configuration for the Titan KV server.
/// Arguments can be passed via command line flags (e.g. --port 6380) 
/// or Environment Variables (e.g. TITAN_PORT=6380).
#[derive(Parser, Debug, Clone)]
#[command(name = "Titan KV")]
#[command(version = "3.0.0")]
#[command(about = "A high-performance Redis-compatible database engine", long_about = None)]
pub struct Config {
    /// The host to bind the server to.
    #[arg(short, long, env = "TITAN_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// The port to listen on.
    #[arg(short, long, env = "TITAN_PORT", default_value = "6379")]
    pub port: u16,

    /// If true, pinning the server to a single CPU core for ultra-low latency.
    #[arg(long, env = "TITAN_SINGLE_THREAD", default_value_t = false)]
    pub single_thread: bool,

    /// The password required to authenticate. If None, the server is public.
    #[arg(long, env = "TITAN_REQUIRE_PASS")]
    pub requirepass: Option<String>,
}
