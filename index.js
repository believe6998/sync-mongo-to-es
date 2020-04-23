require('dotenv').config();
const mongo = require('mongodb');
const es = require('elasticsearch');
const kafka = require('kafka-node');

const dbURL = process.env.DB_CONNECTION;
const dbName = process.env.DB_DATABASE;

const esClient = new es.Client({host: 'localhost:9200'});

const kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
let HighLevelProducer = kafka.HighLevelProducer;
let Consumer = kafka.Consumer;
let ConsumerGroup = kafka.ConsumerGroup;

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
    const pushDataToKafka = (dataToPush, payloadType) => {
        try {
            let payloadToKafkaTopic = [
                {
                    topic: payloadType,
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
    const collection = db.collection('fish');
    let changeStream = collection.watch();
    changeStream.on('change', (change) => {
        if (change.operationType === 'update') {
            let data = change.updateDescription;
            data['_id'] = change.documentKey._id;
            pushDataToKafka(JSON.stringify(data), change.operationType);
        } else {
            pushDataToKafka(change.documentKey._id, change.operationType);
        }
    });

    // collection.find()
    //     .toArray(function (err, cursor) {
    //         cursor.forEach(function (item) {
    //             pushDataToKafka(item);
    //         });
    //     });
})

async function run() {
// let consumer = new Consumer(
//     kafkaClient,
//     [{topic: 'nam', partition: 0}],
//     {autoCommit: true}
// );
// consumer.on('message', function (message) {
//     let data = JSON.parse(message.value);
//     let id = data._id;
//     delete data._id;
//     esClient.index({
//         index: 'fish',
//         id: id,
//         body: data
//     }, function (err, resp, status) {
//         console.log(resp);
//     });
// });

    let options = {
        kafkaHost: 'localhost:9092',
        batch: undefined,
        ssl: true,
        groupId: 'FishGroup',
        sessionTimeout: 15000,
        protocol: ['roundrobin'],
        encoding: 'utf8',
        fromOffset: 'latest',
        commitOffsetsOnFirstJoin: true,
        outOfRangeOffset: 'earliest',
        onRebalance: (isAlreadyMember, callback) => {
            callback();
        }
    };

    let consumerGroup = new ConsumerGroup(options, ['insert', 'update', 'delete']);
    consumerGroup.on('message', function (message) {
        if (message.topic === 'update') {
            let data = JSON.parse(message.value);
            let source = "";
            for (let key in data.updatedFields) {
                source += 'ctx._source["' + key + '"]' + '="' + data.updatedFields[key] + '";';
            }
            esClient.update({
                index: 'fish',
                id: data._id,
                body: {
                    script: {
                        lang: 'painless',
                        inline: source
                    }
                }
            }, function (err, resp, status) {
                if (err) {
                    console.log(err);
                }
            });
        } else {
            let data = message.value;
            if (message.topic === 'insert') {
                esClient.index({
                    index: 'fish',
                    id: data,
                    body: {}
                }, function (err, resp, status) {
                    if (err) {
                        console.log(err);
                    }
                });
            }
            if (message.topic === 'delete') {
                esClient.delete({
                    index: 'fish',
                    id: data
                }, function (err, resp, status) {
                    if (err) {
                        console.log(err);
                    }
                });
            }
        }
    });
}

run().catch(console.log)

