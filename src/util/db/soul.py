from src.util.db.mongodb import connect
from src.settings import ENV

class SoulDBCon:
    @classmethod
    def getHotDB(cls, db):
        if ENV == 'PROD':
            uris = "mongodb://"
        else:
            uris = "mongodb://"
        db = connect(uris)
        return db
