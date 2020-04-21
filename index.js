require('dotenv').config();
const mongo = require('mongodb').MongoClient;
const es = require('elasticsearch');
const kafka = require('kafka-node');

const dbURL = process.env.DB_CONNECTION;
const dbName = process.env.DB_DATABASE;

const esClient = new es.Client({host: 'localhost:9200'});

const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let HighLevelProducer = kafka.HighLevelProducer;
let Consumer = kafka.Consumer;

let producer = new HighLevelProducer(kafkaClient);

mongo.connect(dbURL, {
    useNewUrlParser: true,
    useUnifiedTopology: true
}, (err, client) => {
    if (err) {
        console.error(err);
        return
    }
    const db = client.db(dbName);
    producer.on('ready', function () {
        const pushDataToKafka = (dataToPush) => {
            try {
                let payloadToKafkaTopic = [
                    {
                        topic: 'test',
                        messages: JSON.stringify(dataToPush)
                    }
                ];
                producer.send(payloadToKafkaTopic, (err, data) => {
                    console.log(data);
                });
            } catch (error) {
                console.log(error);
            }
        };
        const collection = db.collection('orders');
        collection.find()
            .toArray(function (err, cursor) {
                cursor.forEach(function (item) {
                    pushDataToKafka(item);
                });
            });
    });

})

let consumer = new Consumer(
    kafkaClient,
    [{topic: 'nam', partition: 0}],
    {autoCommit: true}
);
consumer.on('message', function (message) {
    let data = JSON.parse(message.value);
    let id = data._id;
    delete data._id;
    esClient.index({
        index: 'fish',
        id: id,
        body: data
    }, function (err, resp, status) {
        console.log(resp);
    });
});

