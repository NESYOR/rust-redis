use std::sync::{Arc, Mutex};
use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use crate::store::Store;
use resp::Value::{BulkString,Error,NULL,SimpleString};
use log::*;
mod resp;
mod store;
pub async fn handle_connection(stream: TcpStream,client_store:Arc<Mutex<Store>>) -> Result<()> {
    let mut _buf = [0; 512];
    let mut conn = resp::RespConnection::new(stream);
    loop {
        let value = conn.read_value().await?;

        if let Some(value) = value {
            let (command, args) = value.to_command()?;
            info!("this is command: {}",command.clone());
            let response = match command.to_ascii_lowercase().as_ref() {
                "ping" => resp::Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                "get"=>{
                    if let Some(BulkString(key)) = args.get(0) {
                        println!("store:{}",client_store.lock().unwrap().get(key.to_string()).unwrap());
                        if let Some(value) = client_store.lock().unwrap().get(key.to_string()) {
                            SimpleString(value)
                        }else{
                            NULL
                        }
                    }else{
                        Error("Get requires one argument".to_string())
                    }
                },
                "set"=>{
                    if let (Some(BulkString(key)),Some(BulkString(value))) = (args.get(0),args.get(1)) {
                        client_store.lock().unwrap().set(key.to_string(),value.to_string());
                    SimpleString("OK".to_string())
                    }else {
                        Error("Set requires two arguments".to_string())
                    }
                }
                _ => resp::Value::Error(format!("command not implemented: {}", command)),
            };
            println!("response string:{:?}",response);
            conn.write_value(response).await?;
        } else {
            break;
        }
    }

    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let env = env_logger::Env::default().filter_or("MY_LOG_LEVEL", "info");
    env_logger::Builder::from_env(env).init();
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let mainstore=Arc::new(Mutex::new(store::Store::new()));
    loop {
        let incoming = listener.accept().await;
        let client_store= mainstore.clone();
        match incoming {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    handle_connection(stream,client_store).await.unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
