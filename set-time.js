const kafka = require('kafka-node');
let moment = require('moment');
const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let HighLevelProducer = kafka.HighLevelProducer;
let Consumer = kafka.Consumer;

let producer = new HighLevelProducer(kafkaClient);

let data = []
for (let i = 0; i < 10001; i++) {
    data.push(i)
}
console.log(data)
const pushDataToKafka = () => {
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

pushDataToKafka()

let consumer = new Consumer(
    kafkaClient,
    [{topic: 'my_test'}],
    {autoCommit: true}
);
consumer.on('message', function (message) {
    console.log(message.value)
});

setTimeout(function(){ consumer.pause() }, 1000);

// setInterval(function(){
//     consumer.on('message', function (message) {
//         console.log(message.value)
//     });
//     setTimeout(function(){ consumer.pause() }, 1000);
//     consumer.resume()
// }, 3000);




