require('dotenv').config();
const mqtt = require('mqtt');
const AWS = require('aws-sdk');

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION
});

const dynamoDB = new AWS.DynamoDB.DocumentClient();

const options = {
    host: 'localhost', // Use localhost since the MQTT server is running on the same machine
    port: process.env.MQTT_PORT,
    protocol: process.env.MQTT_PROTOCOL,
    username: process.env.MQTT_USERNAME,  // Add your username here
    password: process.env.MQTT_PASSWORD
};
const client = mqtt.connect(options);

client.on('connect', () => {
  console.log('Connected to MQTT Broker');
  const topic = 'test'; // Change if needed
  client.subscribe(topic, (err) => {
    if (err) console.error('Subscription error:', err);
    else console.log(`Subscribed to topic: ${topic}`);
  });
});

client.on('message', async (topic, message) => {
  try {
    const jsonData = JSON.parse(message.toString());
    console.log('Received Data:', jsonData);

    // Save to MongoDB
    const params = {
      TableName: "DeviceData",
      Item: {
          device_id: jsonData.device_id,
          date_time: `${jsonData.date} ${jsonData.time}`,
          device_type: jsonData.device_type,
          device_name: jsonData.device_name,
          time_zone: jsonData.time_zone,
          latitude: jsonData.latitude,
          longitude: jsonData.longitude,
          software_ver: jsonData.software_ver,
          signal_strength: jsonData.signal_strength,
          valid: jsonData.valid,
          data: jsonData.data // Store the nested object
      }
  };

  // Insert into DynamoDB
  await dynamoDB.put(params).promise();
    console.log('Data saved to Dynamo');
  } catch (error) {
    console.error('Error processing MQTT message:', error);
  }
});

client.on('error', (error) => {
  console.error('MQTT Client Error:', error);
});
