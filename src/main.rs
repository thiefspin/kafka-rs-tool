use crossbeam_channel::{bounded, select, tick, Receiver};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::client::KafkaClient;
use std::{str, env};
use std::thread;
use std::time::Duration;

static TOPICS: &str = "topics";
static CONSUMER: &str = "consume";

fn main() {
    let kafka_hosts: Vec<&str> = vec!["uat-kafka.int.mrdcourier.com:9092"];
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);

    let ctrl_c_events = ctrl_channel().unwrap();
    let ticks = tick(Duration::from_secs(60));

    if args[1] == TOPICS.to_string() {
        let mut client = create_kafka_client(&kafka_hosts);
        client.load_metadata_all().unwrap();
        for topic in client.topics().iter() {
            println!("{:?}", topic.name())
        }
    } else if args[1] == CONSUMER.to_string() {
        thread::Builder::new()
            .name("kafka_consumer".to_string())
            .spawn(move || start_consumer(&kafka_hosts, args[2].as_str(), args[3].parse::<i32>().unwrap()))
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

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(100);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

fn vec_to_string(v: &Vec<&str>) -> Vec<String> {
    v.iter().map(|s| s.clone().to_string()).collect()
}

fn create_kafka_client(hosts: &Vec<&str>) -> KafkaClient {
    KafkaClient::new(vec_to_string(hosts))
}

fn start_consumer(hosts: &Vec<&str>, topic: &str, partition: i32) {
    let mut consumer = create_consumer(vec_to_string(hosts), topic, partition);
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
