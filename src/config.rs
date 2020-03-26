extern crate dirs;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    name: String,
    broker: String,
}

impl KafkaConfig {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn broker(&self) -> &String {
        &self.broker
    }

    fn clone(&self) -> KafkaConfig {
        return KafkaConfig {
            name: self.name.to_string(),
            broker: self.broker.to_string(),
        };
    }
}

pub fn get(key: String) -> Option<KafkaConfig> {
    match dirs::home_dir() {
        Some(path) => {
            let contents = fs::read_to_string(format!("{}/.kafka/.config", path.display()))
                .expect("Something went wrong reading the file");
            let configs: Vec<KafkaConfig> =
                serde_json::from_str(&contents).expect("JSON was not well-formatted");
            let result: Vec<&KafkaConfig> = configs.iter().filter(|c| c.name == key).collect();
            if result.len() != 0 {
                return Some(result[0].clone());
            } else {
                return None
            }
        }
        None => {
            println!("Impossible to get your home dir!");
            return None;
        }
    }}
