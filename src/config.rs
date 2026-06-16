use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "Titan KV")]
#[command(version = "3.0.0")]
#[command(about = "A high-performance Redis-compatible database engine", long_about = None)]

pub struct Config {
    #[arg(short, long, env = "TITAN_HOST", default_value = "127.0.0.1")]
    pub host: String,

    #[arg(short, long, env = "TITAN_PORT", default_value = "6379")]
    pub port: u16,

    #[arg(long, env = "TITAN_SINGLE_THREAD", default_value_t = false)]
    pub single_thread: bool,

    #[arg(long, env = "TITAN_REQUIRE_PASS")]
    pub requirepass: Option<String>,
}
