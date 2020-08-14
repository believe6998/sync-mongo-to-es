"use strict";
const kafka = require('kafka-node');
const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let HighLevelProducer = kafka.HighLevelProducer;

let producer = new HighLevelProducer(kafkaClient);

const pushDataToKafka = (data) => {
    try {
        let payloadToKafkaTopic = [
            {
                topic: 'test',
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

setInterval(function (){
    for (let i = 0; i < 100; i++) {
        pushDataToKafka(i)
    }
},1000)








