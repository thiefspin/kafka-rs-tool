use kafka::client::KafkaClient;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::str;
use std::fmt::Write;
use std::time::Duration;
use kafka::producer::{Producer, Record, RequiredAcks};

pub struct SimpleKafkaClient {
    pub hosts: Vec<String>
}

impl SimpleKafkaClient {

    pub fn create(&self) -> KafkaClient {
        KafkaClient::new(self.hosts.clone())
    }

    pub fn list_topics(&self) -> Vec<String> {
        let mut client = self.create();
        client.load_metadata_all().unwrap();
        return client.topics().iter().map(|topic| topic.name().to_string()).collect();
    }

    pub fn create_consumer(&self, topic: &str, partition: i32) -> Consumer {
        println!("Consumer group set to {}", whoami::username());
        Consumer::from_hosts(self.hosts.clone())
            .with_topic_partitions(topic.to_owned(), &[partition])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_group(whoami::username().to_owned())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap()
    }

    pub fn start_consumer(&self, mut consumer: Consumer, f: &dyn Fn(String) -> ()) {
        loop {
            for ms in consumer.poll().unwrap().iter() {
                for m in ms.messages() {
                    let message = parse_message(m.value);
                    f(message)
                }
                consumer.consume_messageset(ms).unwrap();
            }
            consumer.commit_consumed().unwrap();
        }
    }

    pub fn create_producer(&self) -> Producer {
        return Producer::from_hosts(self.hosts.clone())
            .with_ack_timeout(Duration::from_secs(10))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();
    }

    pub fn produce(&self, mut producer: Producer, topic: String, msg: String) {
        let mut buf = String::with_capacity(2);
        let _ = write!(&mut buf, "{}", msg);
        producer.send(&Record::from_value(&topic, buf.as_bytes())).unwrap();
        buf.clear();
    }
}

fn parse_message(message_bytes: &[u8]) -> String {
    str::from_utf8(&message_bytes).unwrap().to_owned()
}