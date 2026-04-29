mod server;
mod protocol;
mod engine;


fn main() {
    let db = engine::new_db();
    server::run("127.0.0.1:6379", db);
}
