require('dotenv').config();
const mqtt = require('mqtt');
const mongoose = require('mongoose');

const options = {
    host: `${process.env.MQTT_HOST}`,
    port: process.env.MQTT_PORT,
    protocol: `${process.env.MQTT_PROTOCOL}`,
    username: `${process.env.MQTT_USERNAME}`,  // Add your username here
    password: `${process.env.MQTT_PASSWORD}`
};

mongoose.connect(process.env.MONGO_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
});

const db = mongoose.connection;
db.on('error', console.error.bind(console, 'MongoDB connection error:'));
db.once('open', () => console.log('Connected to MongoDB Atlas'));

const deviceDataSchema = new mongoose.Schema({
    device_type: String,
    device_name: String,
    device_id: String,
    date: String,
    time: String,
    time_zone: String,
    latitude: String,
    longitude: String,
    software_ver: String,
    signal_strength: String,
    valid: Boolean,
    data: Object,
});

const DeviceData = mongoose.model('DeviceData', deviceDataSchema);

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
    const newData = new DeviceData(jsonData);
    await newData.save();
    console.log('Data saved to MongoDB Atlas');
  } catch (error) {
    console.error('Error processing MQTT message:', error);
  }
});

client.on('error', (error) => {
  console.error('MQTT Client Error:', error);
});
