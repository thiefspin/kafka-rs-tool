use crossbeam_channel::{bounded, select, tick, Receiver};
use kafka::client::KafkaClient;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::thread;
use std::time::Duration;
use std::{env, str};
mod config;

static TOPICS: &str = "topics";
static CONSUMER: &str = "consume";

fn main() {
    let args: Vec<String> = env::args().collect();
    let c = config::get(args[1].to_string());
    match c {
        Some(conf) => {
            println!("Using {}", conf.name());
            let kafka_hosts: Vec<String> = vec![conf.broker().to_string()];
            println!("{:?}", args);

            let ctrl_c_events = ctrl_channel().unwrap();
            let ticks = tick(Duration::from_secs(60));

            if args[2] == TOPICS.to_string() {
               for topic in list_topics(kafka_hosts) {
                   println!("{}", topic)
               }
            } else if args[2] == CONSUMER.to_string() {
                thread::Builder::new()
                    .name("kafka_consumer".to_string())
                    .spawn(move || {
                        start_consumer(
                            kafka_hosts,
                            args[3].as_str(),
                            args[4].parse::<i32>().unwrap(),
                        )
                    })
                    .unwrap();
            } else {
                println!("No valid option provided")
            }

            loop {
                select! {
                    recv(ticks) -> _ => {
                        println!("Heartbeat");
                    }
                    recv(ctrl_c_events) -> _ => {
                        println!();
                        println!("Goodbye!");
                        break;
                    }
                }
            }
        }
        None => println!("No config file found. Please defer to the README.md for instructions on how to create a config file"),
    }
}

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(100);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

fn list_topics(kafka_hosts: Vec<String>) -> Vec<String> {
    let mut client = create_kafka_client(kafka_hosts);
    client.load_metadata_all().unwrap();
    return client
        .topics()
        .iter()
        .map(|topic| topic.name().to_string())
        .collect();
}

// fn vec_to_string(v: &Vec<&str>) -> Vec<String> {
//     v.iter().map(|s| s.clone().to_string()).collect()
// }

fn create_kafka_client(hosts: Vec<String>) -> KafkaClient {
    KafkaClient::new(hosts)
}

fn start_consumer(hosts: Vec<String>, topic: &str, partition: i32) {
    let mut consumer = create_consumer(hosts, topic, partition);
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let message = parse_message(m.value);
                println!("{}", message)
            }
            consumer.consume_messageset(ms).unwrap();
        }
        consumer.commit_consumed().unwrap();
    }
}

fn create_consumer(hosts: Vec<String>, topic: &str, partition: i32) -> Consumer {
    println!("Consumer group set to {}", whoami::username());
    Consumer::from_hosts(hosts)
        .with_topic_partitions(topic.to_owned(), &[partition])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group(whoami::username().to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap()
}

fn parse_message(message_bytes: &[u8]) -> String {
    str::from_utf8(&message_bytes).unwrap().to_owned()
}
