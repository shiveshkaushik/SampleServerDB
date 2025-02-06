require('dotenv').config();
const mqtt = require('mqtt');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs'); // v3 import for SQS
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb'); // v3 import for DynamoDB

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
    console.log('Received Data:', jsonData);

    // Combine date and time into date_time field
    const date_time = `${jsonData.date} ${jsonData.time}`;
    // Convert time to ISO format
    //const isoDateTime = new Date(`${jsonData.date} ${jsonData.time}`).toISOString();
    // Extract hour from time string (hh:mm:ss)
    const hour = parseInt(jsonData.time.split(':')[0]);
    console.log("For hour: ", hour);

    // Check if time is before 06:00 or after 19:00
    if (hour <= 6 || hour >= 19) {
      // Send to DynamoDB
      const dynamoParams = {
        TableName: "DeviceData",
        Item: {
          device_id: { S: jsonData.device_id },
          date_time: { S: date_time }, // Use ISO 8601 format
          device_type: { S: jsonData.device_type },
          device_name: { S: jsonData.device_name },
          time_zone: { S: jsonData.time_zone },
          latitude: { N: parseFloat(jsonData.latitude) || 0 }, // Convert to number
          longitude: { N: parseFloat(jsonData.longitude) || 0 }, // Convert to number
          software_ver: { S: jsonData.software_ver },
          signal_strength: { N: parseInt(jsonData.signal_strength) || 0 }, // Convert to number
          valid: { BOOL: Boolean(jsonData.valid) }, // Ensure it's a boolean
          data: { M: JSON.parse(JSON.stringify(jsonData.data)) }, // Ensure data is correctly formatted as map
          ttl: { N: (Math.floor(Date.now() / 1000) + 7 * 24 * 60 * 60).toString() } // TTL for 7 days
        }
      };

      console.log("DynamoDB Params:", dynamoParams);
      const dynamoCommand = new PutItemCommand(dynamoParams);
      await dynamoDB.send(dynamoCommand);
    } else {
      const formattedDateTime = date_time.replace(/[\s:/]/g, '-');
      // Send to SQS FIFO queue
      const sqsParams = {
        QueueUrl: process.env.SQS_QUEUE_URL,
        MessageBody: JSON.stringify({
          device_id: jsonData.device_id,
          date_time: date_time,  // Use combined date_time
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
        // Required for FIFO queues
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
