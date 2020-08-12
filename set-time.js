"use strict";
const kafka = require('kafka-node');
const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let Consumer = kafka.Consumer;

let consumer = new Consumer(
    kafkaClient,
    [{topic: 'test'}],
    {
        autoCommit: false
    }
);
let minBatchSize = 10
let buffer = []
consumer.on('message', function (message) {
    buffer.push(message.value)
    if( buffer.length >= minBatchSize){
        consumer.commit((error, data) => {
            if (error) {
                console.error(error);
            } else {
                console.log(buffer)
                console.log('Commit success: ', data);
                buffer = []
            }
        });
    }
});

let timer = setInterval(function(){
    if (buffer.length > 0){
        consumer.commit((error, data) => {
            if (error) {
                console.error(error);
            } else {
                console.log(buffer)
                console.log('Commit success: ', data);
                buffer = []
            }
        });
    }
    }, 10000)












