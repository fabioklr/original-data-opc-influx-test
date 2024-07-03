import logging
import asyncio
import pandas as pd
from asyncua import Server, ua
from datetime import datetime
from dotenv import load_dotenv
import os

async def main():
    load_dotenv()
    _logger = logging.getLogger('asyncua')
    # setup our server
    server = Server()
    await server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840')
    server.set_server_name('Machine Simulator')
    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
            ua.SecurityPolicyType.Basic256Sha256_Sign
        ]
    )

    # setup our own namespace
    uri = 'http://devnetiot.com/opcua/'
    idx = await server.register_namespace(uri)

    # create a new node type we can instantiate in our address space
    trend_names = [f'trend{i}' for i in range(1, 9)]
    trend_objects = {}

    for name in trend_names:
        trend = await server.nodes.objects.add_object(idx, name)
        time_var = await trend.add_variable(idx, 'time', ua.Variant(datetime.now(), ua.VariantType.DateTime))
        value_var = await trend.add_variable(idx, 'value', ua.Variant(0, ua.VariantType.Double))
        trend_objects[name] = {'time': time_var, 'value': value_var}

    sensor_data = pd.read_csv("merged.csv", dtype=str)

    _logger.info('Starting server!')
    async with server:
        while True:
            for row in sensor_data.itertuples(index=False):
                for i, trend_name in enumerate(trend_names):
                    time_var = trend_objects[trend_name]['time']
                    value_var = trend_objects[trend_name]['value']
                    
                    time = row[i*2] # Time values are in the first column for each trend
                    if isinstance(time, float):
                        time = datetime(1980, 1, 1, 0, 0)
                    else:
                        time = datetime.strptime(time, "%d.%m.%y %H:%M:%S")
                        
                    value = row[i*2 + 1] # Trend values are in the second column for each trend
                    if isinstance(value, float):
                        value = float(-999)
                    else:
                        value = float(value)
                
                    _logger.info(f"{trend_name} - Time: {time}, Value: {value}")
            
                    # Writing Variables
                    await time_var.write_value(time)
                    await value_var.write_value(value)
                    
                await asyncio.sleep(5)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
