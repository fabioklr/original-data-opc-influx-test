import asyncio
from asyncua import Client
import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up InfluxDB client
influx_token = os.environ.get("INFLUXDB_TOKEN")
influx_org = 'sieber&partners'
influx_bucket = 'original-data-opc-influx-test'
influx_host = "https://westeurope-1.azure.cloud2.influxdata.com"
influx_client = InfluxDBClient(url=influx_host, token=influx_token, org=influx_org)
write_api = influx_client.write_api()

# Set up OPC UA client
sensor_host_ip = os.environ.get("SENSOR_HOST_IP")
sensor_host = f"opc.tcp://{sensor_host_ip}:4840"
sensor_client = Client(sensor_host)

async def get_data():
    await sensor_client.connect()
    root = sensor_client.nodes.root
    trend_names = [f"trend{i}" for i in range(1, 9)]
    variables = []

    for trend in trend_names:
        trend_node = await root.get_child(["0:Objects", f"2:{trend}"])
        time_var = await trend_node.get_child(["2:time"])
        value_var = await trend_node.get_child(["2:value"])
        timestamp = await time_var.read_value()
        value = await value_var.read_value()
                
        variables.append((timestamp, value))

    await sensor_client.disconnect()
    return variables

def write_data(variables):
    points = []
    for idx, (timestamp, value) in enumerate(variables, start = 1):
        point = Point(f"trend{idx}").field("value", value).time(timestamp, write_precision=WritePrecision.NS)
        points.append(point)

    write_api.write(bucket=influx_bucket, org=influx_org, record=points)
    write_api.close()

async def main():
    variables = await get_data()
    write_data(variables)

if __name__ == "__main__":
    asyncio.run(main())
