# import sys

# import os

# import csv

# import time

# import pymssql

# import distutils.core
import json
import random
import datetime
import time
import csv
from json import JSONDecodeError

import dateutil.parser
import requests
from multiprocessing import Pool
from semanticenrichment import SemanticEnrichment
from ngsi_ld.ngsi_parser import NGSI_Type
import ngsi_ld

### parameters

PAUSE = 0
PROCESSES = 250
# STREAMS = 2
UPDATES = 10
# FIELD = 'mac'
# VALUE = 'B4:E6:2D:8A:20:DD'

# FIELD = 'temperature'
# VALUE = 10
ITERATIONS = 5 #5 #10
STREAMS = [200]#[1, 5, 10, 25, 50, 100, 150, 200, 250]
UPDATES = [10] #[1, 10, 100, 500, 1000, 2500, 5000, 7500, 10000]
FIELDS = {'temperature': 10, 'mac': 'B4:E6:2D:8A:20:DD'}
# FIELDS = {'mac': 'B4:E6:2D:8A:20:DD', 'temperature': 10}
UPDATE_INTERVAL = 0 #5
# FIELDS = {'mac': 'B4:E6:2D:8A:20:DD', 'temperature': 10}

OWN_SE = True

if OWN_SE:
    semanticEnrichment = SemanticEnrichment()


# The available metadata for some of the FIELDS.
META = {
    'updateinterval': 10,
    'unit': 'seconds',
    'humidity': {'min': 0.0, 'max': 100.0, 'valuetype': 'float'},
    'battery': {'min': 0.0, 'max': 100.0, 'valuetype': 'float'},
    'temperature': {'min': -30.0, 'max': 80.0, 'valuetype': 'float'},
    'mac': {'regexp': '(?:[0-9a-fA-F]:?){12}', 'valuetype': 'string'}
}

# Headers used for requests to ngsi-ld broker.
headers = {}
headers.update({'content-type': 'application/ld+json'})
headers.update({'accept': 'application/json'})


def send(data):
    if OWN_SE:
        send_direct(data)
    else:
        send_local(data)


def send_local(ngsi_msg):
    url = "http://localhost:8081/semanticenrichment/callback"
    try:
        r = requests.post(url, json=ngsi_msg, headers=headers)
    except requests.ConnectionError:
        print("Connection could not be established", url)


def send_direct(data):
    # data = request.get_json()
    # logger.debug("callback called" + str(data))
    # print("callback called" + str(data))

    ngsi_type = ngsi_ld.ngsi_parser.get_type(data)

    # check if notification which might contain other entities
    if ngsi_type is NGSI_Type.Notification:
        data = ngsi_ld.ngsi_parser.get_notification_entities(data)
    else:
        data = [data]

    for entity in data:
        # notify about new iotstream, sensor, streamobservation, initialise qoi system if new stream
        semanticEnrichment.notify_datasource(entity)






def sensor_addmetainformation(sensor, field):
    """Adds avilable metadata for a field to a given sensor"""
    if field in META:
        if 'min' in META[field]:
            sensor['qoi:min']['value'] = META[field]['min']
        if 'max' in META[field]:
            sensor['qoi:max']['value'] = META[field]['max']
        if 'valuetype' in META[field]:
            sensor['qoi:valuetype']['value'] = META[field]['valuetype']
        if 'regexp' in META[field]:
            sensor['qoi:regexp']['value'] = META[field]['regexp']

    if 'updateinterval' in META:
        sensor['qoi:updateinterval']['value'] = META['updateinterval']
        if 'unit' in META:
            sensor['qoi:updateinterval']['qoi:unit']['value'] = META['unit']


def stream_build(id, field):
    """Creates a new stream by loading the dummy stream file and setting its name."""
    with open("json/stream.json") as jFile:
        name = id + ":" + field
        stream = json.load(jFile)
        stream['id'] = stream['id'] + name
        return stream


def stream_addsensor(stream, sensor):
    """Adds a sensor to a stream."""
    stream['iot-stream:generatedBy']['object'] = sensor['id']


def observableproperty_build(field, id):
    """Creates a new observable property."""
    with open('json/observableproperty.json') as jFile:
        observableproperty = json.load(jFile)
        observableproperty['id'] = observableproperty['id'] + id + ":" + field
        observableproperty['rdfs:label']['value'] = field.lower()
        return observableproperty


def platform_build(id, coordinates=None):
    """Creates a new platform."""
    with open('json/platform.json') as jFile:
        platform = json.load(jFile)
        platform['id'] = platform['id'] + id
        if coordinates:
            platform['location']['value']['coordinates'] = coordinates
        return platform


def platform_addsensor(platform, sensor):
    """Adds a sensor to a given platform."""
    i = str(sum([1 for k, v in platform.items() if k.startswith('sosa:hosts')]) + 1)
    platform['sosa:hosts#' + i] = {}
    platform['sosa:hosts#' + i]['type'] = 'Relationship'
    platform['sosa:hosts#' + i]['object'] = sensor['id']


def sensor_build(id, field, coordinates=None):
    """Creates a new sensor."""
    with open('json/sensor.json') as jFile:
        sensor = json.load(jFile)
        sensor['id'] = sensor['id'] + id + ":" + field
        if coordinates:
            sensor['location']['value']['coordinates'] = coordinates
        return sensor


def sensor_addobservation(sensor, observation):
    """Adds an observation to a sensor."""
    sensor['sosa:madeObservation']['object'] = observation['id']


def sensor_addobservableproperty(sensor, observableproperty):
    """Adds an obseravable property to a sensor."""
    sensor['sosa:observes']['object'] = observableproperty['id']


def sensor_addplatform(sensor, platform):
    """Adds a platform to a sensor."""
    sensor['sosa:isHostedBy']['object'] = platform['id']


def observation_build(id, field, value, time):
    """Builds an observation."""
    with open("json/observation.json") as jFile:
        observation = json.load(jFile)
        observation['id'] = observation['id'] + id + ":" + field
        observation['sosa:hasSimpleResult']['value'] = value
        # ts = dateutil.parser.parse(time).astimezone(dateutil.tz.tzutc())
        observation['sosa:hasSimpleResult']['observedAt'] = time
        observation['sosa:resultTime']['value'] = time
        return observation


def observation_addstream(observation, stream):
    """Adds a stream to an observation."""
    observation['iot-stream:belongsTo']['object'] = stream['id']


def observation_addsensor(observation, sensor):
    """Adds a sensor to an observation."""
    observation['sosa:madeBySensor']['object'] = sensor['id']


def observation_addobservableproperty(observation, observableproperty):
    """Adds an observable property to an observation."""
    observation['sosa:observedProperty']['object'] = observableproperty['id']


def timed(f):   #from https://stackoverflow.com/questions/5478351/python-time-measure-function
    start = time.time()
    ret = f()
    elapsed = time.time() - start
    return ret, elapsed

def thread(params):
    # print("params", params)
    ngsi_id = str(params[0])
    field = params[1]
    value = params[2]
    nrupdates = params[3]


    # TODO: do something with the list

    responsetimes = []
    platform = platform_build(ngsi_id)
    stream = stream_build(ngsi_id, field)
    observableproperty = observableproperty_build(field, ngsi_id)

    sensor = sensor_build(ngsi_id, field)
    sensor_addmetainformation(sensor, field)
    sensor_addplatform(sensor, platform)
    platform_addsensor(platform, sensor)

    stream_addsensor(stream, sensor)

    observation = observation_build(ngsi_id, field, value, datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
    observation_addstream(observation, stream)
    observation_addsensor(observation, sensor)
    observation_addobservableproperty(observation, observableproperty)

    sensor_addobservableproperty(sensor, observableproperty)
    sensor_addobservation(sensor, observation)

    send(platform)
    send(stream)
    send(observableproperty)
    send(sensor)
    send(observation)

    for i in range(0,nrupdates):
        observation = observation_build(ngsi_id, field, value, datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
        observation_addstream(observation, stream)
        observation_addsensor(observation, sensor)
        observation_addobservableproperty(observation, observableproperty)
        # print(observation)
        # send_local(observation)
        responsetimes.append(timed(lambda: send(observation))[1])
        time.sleep(UPDATE_INTERVAL)
    print("thread {} finished".format(ngsi_id))
    print("avg response time:", sum(responsetimes)/len(responsetimes))
    return sum(responsetimes)/len(responsetimes)

def main():
    # from guppy import hpy
    # h = hpy()
    print("Sleep", PAUSE, "seconds")
    time.sleep(PAUSE)

    for nr_streams in STREAMS:
        # print(h.heap())
        aList = []
        aList.extend(range(0, nr_streams))


        count = len(aList)

        if count > 0:
            if PROCESSES > nr_streams:
                procs = nr_streams
            else:
                procs = PROCESSES
        pool = Pool(processes=procs)

        for i in range(0,ITERATIONS):
            for nrupdates in UPDATES:
                for key, value in FIELDS.items():
                    # create chunked list for processes

                    aList = [[i, key, value, nrupdates] for i in range(procs)]
                    # print(aList)

                    # spawn threads
                    data = pool.map(thread, aList)

                    filename = 'benchmarking/results.csv'
                    file_exists = os.path.isfile(filename)
                    with open(filename, 'a') as resultFile:
                        csvwriter = csv.writer(resultFile)
                        if not file_exists:
                            csvwriter.writerow(['procs', 'STREAMS', 'UPDATES', 'FIELD', 'UPDATE_INTERVAL', 'data'])
                        csvwriter.writerow([procs, nr_streams, nrupdates, key, UPDATE_INTERVAL, sum(data)/len(data)])

                    print(key, "done")
                print("Nr streams", nr_streams, "done")
            print("Updates", nrupdates, "done")
        print("Iteration", i, "done")
        del pool
    print("...all done")


if __name__ == "__main__":
    import os

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    main()
