"use strict";
const kafka = require('kafka-node');
const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let HighLevelProducer = kafka.HighLevelProducer;

let producer = new HighLevelProducer(kafkaClient);

const pushDataToKafka = (data) => {
    try {
        let payloadToKafkaTopic = [
            {
                topic: 'my_test',
                messages: data
            }
        ];
        producer.send(payloadToKafkaTopic, (err, data) => {
            if (err) {
                console.log(err);
            }
        });
    } catch (error) {
        console.log(error);
    }
};

// pushDataToKafka(data)








