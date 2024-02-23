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
        """
        Connect with the mongodb and set the attribute client.
        """
        self.client = pymongo.MongoClient(self.uri)
        pass

    def get_db(self):
        """
        Return database name.
        """
        return self.client[self.name]

    def get_coll(self, coll):
        "Return collection name."
        return self.get_db()[coll]

    def get_coll_df(self):
        """
        Return collection df.
        """
        return self.get_coll(self.coll_df)

    def get_coll_raw(self):
        """
        Return collection raw.
        """
        return self.get_coll(self.coll_raw)

    def get_coll_main(self):
        """
        Return collection main.
        """
        return self.get_coll(self.coll_main)

    def insert_doc(self, doc, coll):
        """
        Insert document into the collection.
        """
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
        """
        Insert documents into the collection.
        """
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
        """
        Update collection document values.
        """
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
        """
        Fetch documents, this method returns a cursor iterable.
        """
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
        """
        Remove document using the proper filter.
        """
        try:
            self.connect()
            self.get_coll(column).delete_one(filter, collation)
        except Exception as e:
            log.info(f"Exception {e}")
        else:
            pass
        finally:
            pass
