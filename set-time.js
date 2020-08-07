"use strict";
const kafka = require('kafka-node');
const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let Consumer = kafka.Consumer;

let consumer = new Consumer(
    kafkaClient,
    [{topic: 'my_test'}],
    {autoCommit: true}
);
consumer.on('message', function (message) {
    console.log(message.value)
    consumer.pause()
});

setInterval( () => {
    consumer.resume()
}, 1000*30)










