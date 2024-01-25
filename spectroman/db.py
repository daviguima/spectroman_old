import pymongo

from spectroman.log import log
from spectroman.conf import conf

class Db:
    def __init__(self, uri=None, name=None):
        self.uri = uri or conf['DB_URI']
        self.name = name or conf['DB_NAME']
        self.coll_df = conf['DB_COLL_DF']
        self.coll_raw = conf['DB_COLL_RAW']
        self.coll_main = conf['DB_COLL_MAIN']
        self.client = None

    def connect(self):
        self.client = pymongo.MongoClient(self.uri)
        pass

    def get_db(self):
        return self.client[self.name]

    def get_coll(self, coll):
        return self.get_db()[coll]

    def get_coll_df(self):
        return self.get_coll(self.coll_df)

    def get_coll_raw(self):
        return self.get_coll(self.coll_raw)

    def get_coll_main(self):
        return self.get_coll(self.coll_main)

    def insert(self, data, coll):
        try:
            self.get_coll(coll).insert_one(data).inserted_id
        except Exception as e:
            log.info(f"Exception {e}")
        finally:
            log.info(f"Document inserted on the collection {coll}")
            pass

    def fetch(self, query, coll):
        return self.get_coll(coll).find(query)
