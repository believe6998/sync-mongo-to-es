const kafka = require('kafka-node');
const {ClickHouse} = require('clickhouse');
let moment = require('moment');
const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let HighLevelProducer = kafka.HighLevelProducer;
let Consumer = kafka.Consumer;

const ch = new ClickHouse({
    host: 'localhost',
    port: 8123,
    user: 'default',
    password: 1
})
 function isJson(str) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}

let producer = new HighLevelProducer(kafkaClient);
const pushDataToKafka = (dataToPush) => {
    try {
            let payloadToKafkaTopic = [
                {
                    topic: 'demo_ch',
                    messages: dataToPush
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
// se lấy từ khóa người dùng nhập ở đây
let data = {
    'keyword': 'chung cư hola',
    'userId': 113
}
pushDataToKafka(JSON.stringify(data));

let consumer = new Consumer(
    kafkaClient,
    [{topic: 'demo_ch', partition: 0}],
    {autoCommit: true}
);
consumer.on('message', function (message) {
    if (isJson(message.value)){
        let data = JSON.parse(message.value);
        let query = `INSERT INTO demo_ch FORMAT Values ('${moment().format('YYYY-MM-DD')}','${data.keyword}',${data.userId})`
        ch.query(query).exec(function (err) {
            if (err) {
                console.log(err);
            }
        });
    }
});

ch.query('SELECT * FROM demo_ch').exec(function (err, rows) {
    console.log(rows)
});



