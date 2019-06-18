import uuid


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

    def add_subscription(self, host, port, subscription):
        #subscribe to ngsi-ld endpoint
        subscription = Subscription(host, port, subscription)
        self.subscriptions[subscription.id] = subscription

        #TODO send subscription to ngsi broker

    def del_subscription(self, subid):
        datasource = self.subscriptions.pop(subid)

        #TODO unsubscribe at ngsi broker


    def get_subscriptions(self):
        return self.subscriptions