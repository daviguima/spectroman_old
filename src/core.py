import os
import sys

import numpy
import pandas
import matplotlib

from data import *
from model import *

from db import Db
from ftp import Ftp
from log import log
from conf import conf

class Spectroman:
    def __init__(self):
        self.db = Db()
        self.ftp = Ftp()
        # self.plot = Plot(self.db)
        return

    def calc_rss(self, df):
        """
        Given a data frame, compute the RRS values and
        return it.
        """
        df['Rrs650'] = df[[r_ed,
                           r_ld,
                           r_lu]].apply(calc_rrs650_reflectance, axis=1)

        df['Rrs850'] = df[[ir_ed,
                           ir_ld,
                           ir_lu]].apply(calc_rrs850_reflectance, axis=1)

        df['css'] = df[['Rrs850',
                        'Rrs650']].apply(css_jirau, axis=1)
        return df

    def process_file(self, file):
        """
        Given a csv files, compute the RRS values over them
        and persist it using the database module.
        """
        try:
            df = csv_to_df(file)
        except Exception as e:
            log.info(e)
        else:
            if (not df.empty) and len(df) > 1:
                self.db.insert(df.to_dict('list'), self.db.coll_raw)

            df = process_df(df)
            if (not df.empty) and len(df) > 1:
                self.db.insert(self.calc_rss(df).to_dict('list'),
                               self.db.coll_df)
        finally:
            pass

    def process_files(self, files):
        """
        Given a list of csv files, compute the RRS values over them
        and persist it using the database module.
        """
        n = len(files)
        # process each file
        for i, f in enumerate(files):
            log.info(f'Processing: {i + 1}/{n}')
            self.process_file(f)
            # processing complete
        log.info(f'Processing Completed: Rrs650, Rrs650 and CSS.')
        pass
