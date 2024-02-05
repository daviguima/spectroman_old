import pymongo

from log import log
from conf import conf

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

    def insert_doc(self, doc, coll):
        try:
            self.connect()
            self.get_coll(coll).insert_one(doc)
        except Exception as e:
            log.info(f"Exception {e}")
        else:
            log.info(f"Document inserted on the collection {coll}")
        finally:
            pass

    def insert_docs(self, docs, coll):
        try:
            self.connect()
            self.get_coll(coll).insert_many(docs)
        except Exception as e:
            log.info(f"Exception {e}")
        else:
            log.info(f"Document inserted on the collection {coll}")
        finally:
            pass

    def update_doc(self, filter, values, coll):
        try:
            self.connect()
            self.get_coll(coll).update_one(filter, values)
        except Exception as e:
            log.info(f"Exception {e}")
        else:
            log.info(f"Document updated on the collection {coll}")
        finally:
            pass

    def fetch_docs(self, filter, projection, coll):
        cursor = None
        try:
            self.connect()
            cursor = self.get_coll(coll).find(filter, projection)
        except Exception as e:
            log.info(f"Exception {e}")
        else:
            pass
        finally:
            return cursor

    def remove_doc(self, filter, collation, column):
        try:
            self.connect()
            self.get_coll(column).delete_one(filter, collation)
        except Exception as e:
            log.info(f"Exception {e}")
        else:
            pass
        finally:
            pass
