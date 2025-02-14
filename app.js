require('dotenv').config();
const mqtt = require('mqtt');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');

// Initialize AWS SDK clients with credentials
const awsConfig = {
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
};

const sqs = new SQSClient(awsConfig);
const dynamoDB = new DynamoDBClient(awsConfig);

// MQTT connection options
const options = {
  host: process.env.MQTT_HOST,
  port: process.env.MQTT_PORT,
  protocol: process.env.MQTT_PROTOCOL,
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
};

const client = mqtt.connect(options);

client.on('connect', () => {
  console.log('Connected to MQTT Broker');
  const topic = 'test'; // Change topic if needed
  client.subscribe(topic, (err) => {
    if (err) console.error('Subscription error:', err);
    else console.log(`Subscribed to topic: ${topic}`);
  });
});

client.on('message', async (topic, message) => {
  try {
    const jsonData = JSON.parse(message.toString());

    const date_time = `${jsonData.date} ${jsonData.time}`;
    const hour = parseInt(jsonData.time.split(':')[0]);
    const data = jsonData.data;

    if (hour <= 6 || hour >= 19) {
      const dynamoParams = {
        TableName: "DeviceData",
        Item: {
          device_id: { S: jsonData.device_id },
          date_time: { S: date_time },
          device_type: { S: jsonData.device_type },
          device_name: { S: jsonData.device_name },
          time_zone: { S: jsonData.time_zone },
          latitude: { N: String(parseFloat(jsonData.latitude) || 0) },
          longitude: { N: String(parseFloat(jsonData.longitude) || 0) },
          software_ver: { S: jsonData.software_ver },
          signal_strength: { N: String(parseInt(jsonData.signal_strength) || 0) },
          valid: { BOOL: Boolean(jsonData.valid) },
          data: { S: JSON.stringify(data) },
          ttl: { N: String(Math.floor(Date.now() / 1000) + 7 * 24 * 60 * 60) }
        }
      };

      const command = new PutItemCommand(dynamoParams);
      await dynamoDB.send(command);
      console.log('Data sent to DynamoDB');
    } else {
      const formattedDateTime = date_time.replace(/[\s:/]/g, '-');
      const sqsParams = {
        QueueUrl: process.env.SQS_QUEUE_URL,
        MessageBody: JSON.stringify({
          device_id: jsonData.device_id,
          date_time: date_time,
          device_type: jsonData.device_type,
          device_name: jsonData.device_name,
          time_zone: jsonData.time_zone,
          latitude: jsonData.latitude,
          longitude: jsonData.longitude,
          software_ver: jsonData.software_ver,
          signal_strength: jsonData.signal_strength,
          valid: jsonData.valid,
          data: jsonData.data
        }),
        MessageGroupId: jsonData.device_id,
        MessageDeduplicationId: `${jsonData.device_id}-${formattedDateTime}`
      };

      const sqsCommand = new SendMessageCommand(sqsParams);
      await sqs.send(sqsCommand);
      console.log('Data sent to SQS');
    }
  } catch (error) {
    console.error('Error processing MQTT message:', error);
  }
});

client.on('error', (error) => {
  console.error('MQTT Client Error:', error);
});