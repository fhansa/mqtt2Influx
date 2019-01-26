#!/usr/bin/env python
import argparse
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json
#import re
import logging
import sys
import os
import requests.exceptions


class nodeConfig():
    def __init__(self, json):
        self.json = json

    @property   
    def topic(self):
        return self.json["topic"]
    @property
    def name(self):
        return self.json["name"]
    @property
    def measurement(self):
        return self.json["measurement"]



class MQTTSource:

    logger = logging.getLogger("influx2Mqtt.MQTTSource")

    def __init__(self, host, port, node_config, influx):
        self.host = host
        self.port = int(port)
        self.node_config = node_config
        self.influx = influx
        self.sensors = []
        self.logger.debug(node_config)
        self.parseSensors(node_config)
        self.setupMQTT()

    def parseSensors(self, config):
        for sensor in config["sensors"]:
            self.logger.debug(sensor)
            self.sensors.append(nodeConfig(sensor))

    def setupMQTT(self):
        self.client = mqtt.Client()

        def on_connect(client, userdata, flags, rc):
            self.logger.info("MQTT connected")
            for sensor in self.sensors:
                self.logger.info("Subscribing to topic %s for node %s", sensor.topic, sensor.name)
                client.subscribe(sensor.topic)

        def on_message(client, userdata, msg):
            self.logger.debug(
                "MQTT message - Topic: %s Payload: %s", msg.topic, msg.payload)
            
            for sensor in self.sensors:
                if sensor.topic == msg.topic:
                    is_json = False
                    try:
                        payload = json.loads(msg.payload)
                        is_json = isinstance(payload, dict)
                    except ValueError:
                        pass

                    if (is_json):
                        pass
                    else:
                        value = { "value" : float(payload) }

                    self.influx.store_msg(sensor.name, sensor.measurement, "", value)
                    

        self.client.on_connect = on_connect
        self.client.on_message = on_message

    def start(self):
        self.client.connect(self.host, self.port)
        self.client.loop_forever()

class influxStore:

    logger = logging.getLogger("mqtt2influx.influxStore")

    def __init__(self, host, port, username, password, database):
        self.influx_client = InfluxDBClient(
            host=host, port=port, username=username, password=password, database=database)

    def store_msg(self, sensor, measurement, tags, data):
        if not isinstance(data, dict):
            self.logger.exception("Wrong data input")
            raise ValueError('Wrong data, must be dictionary')
        influx_msg = {
            'measurement': measurement,
            'tags': {
                'sensor_node': sensor,
            },
            'fields': data
        }
        self.logger.debug(influx_msg)
#        for t in tags:
#            influx_msg["tags"].append(t)
        try:
            self.influx_client.write_points([influx_msg])
        except requests.exceptions.ConnectionError as e:
            self.logger.exception(e)



def main():
    logger = logging.getLogger("mqtt2influx.main")

    parser = argparse.ArgumentParser(description='MQTT to InfluxDB parser')
    parser.add_argument('--mqtt-host', required=True, help='MQTT host')
    parser.add_argument('--mqtt-port', default="1883", help='MQTT port')
    parser.add_argument('--influx-host', required=True, help='InfluxDB host')
    parser.add_argument('--influx-port', default="8086", help='InfluxDB port')
    parser.add_argument('--influx-db', required=True, help='InfluxDB database')
    parser.add_argument('--verbose', help='Enable verbose output to stdout', default=False, action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    try: 
        dirname, filename = os.path.split(os.path.abspath(__file__))
        with open(dirname + '/config.json') as json_data_file:
            config = json.load(json_data_file)
    except:
        logger.exception("Cannot read config.json")

    influx=influxStore(args.influx_host, args.influx_port, "", "", args.influx_db)
    mqtt = MQTTSource(args.mqtt_host, args.mqtt_port, config, influx)
    mqtt.start() 

if __name__ == '__main__':
    main()