import uuid
import requests

class Subscription:
    def __init__(self, host, port, subscription):
        self.id = uuid.uuid4()
        self.host = host
        self.port = port
        self.subscription = subscription


class DataSource:
    def __init__(self, id, metadata):
        self.id = id
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
        sub = Subscription(host, port, subscription)
        self.subscriptions[sub.id] = sub

        server_url = host + ":" + str(port) + "/ngsi-ld/v1/subscriptions"
        r = requests.post(server_url, json=subscription, headers=self.headers)
        print("add_subscription", r.text)
        return r.text

        #TODO send subscription to ngsi broker

    def del_subscription(self, subid):
        print(self.subscriptions.keys())
        datasource = self.subscriptions.pop(uuid.UUID(subid))

        #TODO unsubscribe at ngsi broker

    def add_datasource(self, data):
        print("add_datasource with", data)
        #TODO parse and add to store
        #TODO check how to get the data

    def get_subscriptions(self):
        return self.subscriptions

    def get_datasources(self):
        return self.datasources