import uuid
import requests
import json

class Subscription:
    def __init__(self, subid, host, port, subscription):
        self.id = subid
        self.host = host
        self.port = port
        self.subscription = subscription



class DataSource:
    def __init__(self, id, dstype, metadata):
        self.id = id
        self.dstype = dstype
        self.metadata = metadata

class DatasourceManager:

    def __init__(self):
        self.subscriptions = {}
        self.datasources = {}

        self.headers = {}
        self.headers.update({'content-type': 'application/ld+json'})
        self.headers.update({'accept': 'application/ld+json'})

    def add_subscription(self, host, port, subscription):
        #subscribe to ngsi-ld endpoint
        print("test", subscription)
        sub = Subscription(subscription['id'], host, port, subscription)
        self.subscriptions[sub.id] = sub

        server_url = host + ":" + str(port) + "/ngsi-ld/v1/subscriptions/"
        print(subscription)
        r = requests.post(server_url, json=subscription, headers=self.headers)
        print("add_subscription", r.text)
        return r.text


    def del_subscription(self, subid):
        print(self.subscriptions.keys())
        subscription = self.subscriptions.pop(subid)

        #TODO unsubscribe at ngsi broker
        server_url = subscription.host + ":" + str(subscription.port) + "/ngsi-ld/v1/subscriptions/"
        server_url = server_url + subid
        print(server_url)
        r = requests.delete(server_url, headers=self.headers)
        print("del_subscription", r)

    def add_datasource(self, data):
        print("add_datasource with", data)
        #TODO parse and add to store
        datasource = DataSource(data['id'], data['type'], data)
        self.datasources[data['id']] = datasource
        #TODO check how to get the data

    def get_subscriptions(self):
        return self.subscriptions

    def get_datasources(self):
        return self.datasources