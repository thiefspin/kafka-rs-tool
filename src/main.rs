use crate::kafka_client::SimpleKafkaClient;
use crate::config::KafkaConfig;
use crossbeam_channel::{bounded, select, Receiver};
use std::thread;
use std::{env, str};

mod config;
mod kafka_client;

static TOPICS: &str = "topics";
static CONSUMER: &str = "consume";

fn main() {
    let args: Vec<String> = env::args().collect();
    let c: Option<KafkaConfig> = config::get(args[1].to_string());
    let ctrl_c_events = ctrl_channel().unwrap();
    match c {
        Some(conf) => {
            start(args, conf);
            keep_alive(ctrl_c_events)
        }
        None => println!("No config file found. Please defer to the README.md for instructions on how to create a config file"),
    }
}

fn start(args: Vec<String>, conf: KafkaConfig) {
    println!("Using {}", conf.name());
    let kafka_hosts: Vec<String> = vec![conf.broker().to_string()];
    println!("{:?}", args);

    let client = SimpleKafkaClient{
        hosts: kafka_hosts.clone()
    };

    if args[2] == TOPICS.to_string() {
        print_topics(client.list_topics())
    } else if args[2] == CONSUMER.to_string() {
        start_consumer_thread(args, client)
    } else {
        println!("No valid option provided")
    }
}

fn keep_alive(ctrl_c_events: Receiver<()>) {
    loop {
        select! {
            recv(ctrl_c_events) -> _ => {
                println!();
                println!("Goodbye!");
                break;
            }
        }
    }
}

fn print_topics(topics: Vec<String>) {
    for topic in topics {
        println!("{}", topic)
    }
}

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(100);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

fn message_handler(msg: String) {
    println!("{}", msg)
}

fn start_consumer_thread(args: Vec<String>, client: SimpleKafkaClient) {
    thread::Builder::new()
        .name("kafka_consumer".to_string())
        .spawn(move || {
            let consumer = client.create_consumer(args[3].as_str(), args[4].parse::<i32>().unwrap());
            client.start_consumer(consumer, &message_handler)
        })
        .unwrap();
}
