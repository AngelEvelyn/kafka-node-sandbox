const kafka = require('kafka-node');
const bp = require('body-parser');
const config = require('./config');

try {
  let consumerGroup = new kafka.ConsumerGroup(
    { 
      groupId: 'testingLab',     
      kafkaHost: 'localhost: 9093',
      //fromOffset: 'earliest'
    },
    config.kafka_topic
  );
  // const client = new kafka.Client(config.kafka_server);
  // const Consumer = kafka.HighLevelConsumer;
  // let consumer = new Consumer(
  //   client,
  //   [{ topic: config.kafka_topic, partition: 0 }],
  //   {
  //     autoCommit: true,
  //     fetchMaxWaitMs: 1000,
  //     fetchMaxBytes: 1024 * 1024,
  //     encoding: 'utf8',
  //     fromOffset: false
  //   }
  // );
  // let consumer = new kafka.Consumer(client, 
  //   [{
  //     topic: config.kafka_topic
  //   }],
  //   {
  //   groupId: 'testingLab'
  // })
  consumerGroup.on('message', async function(message) {
    console.log('Message received');
    console.log(
      'kafka-> ',
      message
      //message.value
    );
  })
  consumerGroup.on('error', function(err) {
    console.log('error', err);
  });
}
catch(e) {
  console.log(e);
}