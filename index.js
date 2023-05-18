const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'laundry-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'laundry-group' })

const run = async () => {
  // Producing
  await producer.connect()
  await producer.send({
    topic: 'laundry-group',
    messages: [
      { value: 'Hello from Laundry Service Corp!' },
    ],
  })

  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'laundry-machine-logs', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}

run();
