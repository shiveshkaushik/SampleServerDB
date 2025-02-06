import dotenv from 'dotenv';
dotenv.config();
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb'; // v3 import for DynamoDB
const awsConfig = {
    region: process.env.AWS_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
  };
  const dynamoDB = new DynamoDBClient(awsConfig);
  const params = {
    TableName: "DeviceData",
    Item: {
      device_id: { S: 'hello' },
      date_time: { S: '06/02/2025 19:53:37' },
      device_type: { S: 'invtr' },
      device_name: { S: 'invt' },
      time_zone: { S: 'Asia/Kolkata' },
      latitude: { S: '0' },
      longitude: { S: '0' },
      software_ver: { S: 'B6.M.61_4G' },
      signal_strength: { S: '5' },
      valid: { BOOL: false },
      data: { M: { // Nested object (map)
        slave_id: { S: '1' },
        serial_no: { S: '0' },
        state: { N: '0' },
        pv1_voltage: { N: '0' },
        pv2_voltage: { N: '0' },
        pv3_voltage: { N: '0' },
        pv1_current: { N: '0' },
        pv2_current: { N: '0' },
        pv3_current: { N: '0' },
        frequency: { N: '0' },
        grid_voltage_v1: { N: '0' },
        grid_voltage_v2: { N: '0' },
        grid_voltage_v3: { N: '0' },
        grid_current_i1: { N: '0' },
        grid_current_i2: { N: '0' },
        grid_current_i3: { N: '0' },
        total_output_power: { N: '0' },
        today_e: { N: '0' },
        total_e: { N: '0' },
        inverter_temp: { N: '0' }
      }},
      ttl: { N: '1739456642' }
    }
  };
  const dynamoCommand = new PutItemCommand(params);
await dynamoDB.send(dynamoCommand);