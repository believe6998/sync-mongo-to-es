'use strict';
const {Kafka} = require('kafkajs')

const kafka = new Kafka({
    brokers: ['localhost:9092'],
    clientId: 'example-consumer'
})
const consumer = kafka.consumer({
    groupId: 'my-group',
    maxWaitTimeInMs: 30000,
    minBytes: 100,
    sessionTimeout: 60*1000
})

const run = async () => {
    await consumer.connect()
    await consumer.subscribe({topic: 'test', fromBeginning: true})
    await consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: async ({
                              batch,
                              resolveOffset,
                              heartbeat
                          }) => {
            let rs = []
            for (let message of batch.messages) {
                rs.push(message.value.toString())
                resolveOffset(message.offset)
                await heartbeat()
            }
            console.log(rs)
        },
    })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

