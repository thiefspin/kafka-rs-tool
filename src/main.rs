use crossbeam_channel::{bounded, select, tick, Receiver};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::str;
use std::thread;
use std::time::Duration;

fn main() {
    thread::Builder::new()
        .name("kafka_consumer".to_string())
        .spawn(|| start_consumer())
        .unwrap();
    let ctrl_c_events = ctrl_channel().unwrap();
    let ticks = tick(Duration::from_secs(60));

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

fn start_consumer() {
    let mut consumer = create_consumer(vec!["localhost:9092".to_string()]);
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let message = parse_message(m.value);
                println!("{:?}", message)
            }
            consumer.consume_messageset(ms).unwrap();
        }
        consumer.commit_consumed().unwrap();
    }
}

fn create_consumer(hosts: Vec<String>) -> Consumer {
    Consumer::from_hosts(hosts.to_owned())
        .with_topic_partitions("example-topic".to_owned(), &[0])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my-group".to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap()
}

fn parse_message(message_bytes: &[u8]) -> String {
    str::from_utf8(&message_bytes).unwrap().to_owned()
}
