import logging

from pymongo import MongoClient
from pymongo import ReadPreference
from pymongo import uri_parser

__all__ = ['connect']

_LOG = logging.getLogger(__name__)

# class --------------------------------------------------------------------------------------------
class MongoDB(object):
    def __init__(self, uri=None):
        self.uri = None
        self.client = None
        if uri:
            self.uri = uri
            self.client = MongoClient(uri)
            _LOG.debug('Connected to MongoDB : ' + uri)

    def init(self):
        self.uri = None
        self.client = None

    def database(self, databaseName):
        if not self.client:
            self.client = MongoClient(self.uri)
        return self.client.get_database(name=databaseName, read_preference=ReadPreference.SECONDARY_PREFERRED)

    def connect(self, mongodbUri):
        if mongodbUri is None:
            return None
        self.uri = mongodbUri
        parsed = uri_parser.parse_uri(mongodbUri)
        return self.database(parsed['database'])

    def serverStatus(self):
        return self.client.server_info()

    def __getitem__(self, item):
        return self.database(item)

    def __getattr__(self, item):
        return self.database(item)

# method -------------------------------------------------------------------------------------------
def connect(toMongodbUri):
    if toMongodbUri is None:
        return None
    parsed = uri_parser.parse_uri(toMongodbUri)
    return MongoDB(toMongodbUri).database(parsed['database'])
