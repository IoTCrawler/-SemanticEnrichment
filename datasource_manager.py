import uuid


class Datasource:
    def __init__(self, host, port, subscription):
        self.id = uuid.uuid4()
        self.host = host
        self.port = port
        self.subscription = subscription


class DatasourceManager:

    def __init__(self):
        self.datasources = {}

    def add_datasource(self, host, port, subscription):
        #subscribe to ngsi-ld endpoint
        datasource = Datasource(host, port, subscription)
        self.datasources[datasource.id] = datasource

        #TODO send subscription to ngsi broker

    def del_subscription(self, subid):
        datasource = self.datasources.pop(subid)

        #TODO unsubscribe at ngsi broker


    def get_subscriptions(self):
        return self.datasources