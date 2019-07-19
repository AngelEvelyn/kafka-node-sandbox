const kafka = require('kafka-node');
const bp = require('body-parser');
const config = require('./config');

const topics = ['first_topic', 'second_topic', 'second_topic', 'third', 'forth', 'second_message'];

topics.forEach(topicName => {
  try {
    const Producer = kafka.Producer;
    const client = new kafka.Client(config.kafka_server);
    const producer = new Producer(client, {partitionerType: 2});
    const kafka_topic = config.kafka_topic;
    console.log(kafka_topic);
    let payloads = [
      {
        topic: topicName,
        messages: Math.random(),
        timestamp: Date.now(),
      }
    ];
  
    producer.on('ready', async function() {
      setInterval(() => {
        producer.send(payloads, (err, data) => {
          if (err) {
            console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
          } else {
            console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
          }
        });
      }, 100)
    });
  
    producer.on('error', function(err) {
      console.log(err);
      console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
      throw err;
    });
  }
  catch(e) {
    console.log(e);
  }
})