const kafka = require('kafka-node');
const config = require('./config');

const cliTopics = process.argv.slice(2);
const topics = cliTopics.length > 0 ? cliTopics : config.topics;

topics.forEach(topicName => {
  try {
    const Producer = kafka.Producer;
    const client = new kafka.Client(config.kafkaServer);
    const producer = new Producer(client, {partitionerType: 2});
    console.log(topicName);
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
            console.log('[kafka-producer -> '+topicName+']: broker update failed');
          } else {
            console.log('[kafka-producer -> '+topicName+']: broker update success');
          }
        });
      }, config.intervalInMs)
    });
  
    producer.on('error', function(err) {
      console.log(err);
      console.log('[kafka-producer -> '+topicName+']: connection errored');
      throw err;
    });
  }
  catch(e) {
    console.log(e);
  }
})