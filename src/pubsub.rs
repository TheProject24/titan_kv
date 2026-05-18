use std::collections::HashMap;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
// use std::fmt::format;
use std::sync::Arc;
use tokio::sync::{
    broadcast,
    RwLock
};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub type PubSub = Arc<RwLock<HashMap<String, broadcast::Sender<String>>>>;

pub fn new_pubsub() -> PubSub {
    Arc::new(RwLock::new(HashMap::new()))
}

pub async fn handle_publish(pubsub: &PubSub, channel: &str, message: &str, stream: &mut OwnedWriteHalf) {
    let map = pubsub.read().await;

    if let Some(sender) = map.get(channel) {
        let receivers = sender.send(message.to_string()).unwrap_or(0);
        let response = format!(":{}\r\n", receivers);
        let _ = stream.write_all(response.as_bytes()).await;
    } else {
        let _ = stream.write_all(b":0\r\n").await;
    }
}

pub async fn handle_subscribe(
    pubsub: &PubSub, 
    channel: &str, 
    stream: &mut OwnedWriteHalf,
    reader: &mut BufReader<OwnedReadHalf>
) {
    let mut rx = {
        let mut map = pubsub.write().await;
        let sender = map.entry(channel.to_string()).or_insert_with(|| {
            let (tx, _rx) = broadcast::channel::<String>(100);
            tx
        });

        sender.subscribe()
    };

    let ack = format!("*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:1\r\n", channel.len(), channel);
    let _ = stream.write_all(ack.as_bytes()).await;

    loop {
        let mut line = String::new();

        tokio::select! {
            msg_result = rx.recv() => {
                match msg_result {
                    Ok(msg) => {
                        let payload = format!(
                            "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n", 
                            channel.len(),
                            channel,
                            msg.len(),
                            msg
                        );
                        if stream.write_all(payload.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }

            read_result = reader.read_line(&mut line) => {
                match read_result {
                    Ok(0) => break,
                    Ok(_) => {
                        let clean_line = line.trim();

                        if clean_line.to_uppercase().contains("UNSUBSCRIBE") {
                            let ack = format!(
                                "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:0\r\n",
                                channel.len(),
                                channel
                            );
                            let _ = stream.write_all(ack.as_bytes()).await;
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}