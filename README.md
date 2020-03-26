# Simple Kafka commandline tool

## Usage 
The tool supports various Kafka environments. This can be defined by a config file in JSON format.
The file needs to be located in 
```
$HOME/.kafka/.config
```

Example config file:
```json
[
  {
    "name": "local",
    "broker": "localhost:9092"
  }
]
```

### Listing topics
The following command will provide you with topics from the relevant broker information specified by the config file:
```shell script
kafka-rs local topics
```

### Consuming from a topic
The following command will provide you with topics from the relevant broker information specified by the config file:
```shell script
kafka-rs $identifier consume $topic $partition
kafka-rs local consume test-topic 0
```