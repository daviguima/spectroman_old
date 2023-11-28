import os
import sys

from data import *
from model import *

# from db import Db
from log import log
from ftp import Ftp
from conf import conf

class Spectroman:
    def __init__(self):
        return
    # self.db = Db()
    # self.Ftp = Ftp()
    # self.option = Modes(self)

    def compute_values(self, file):
        for _, row in process_df(csv_to_df(file)).iterrows():
            r_ed=row['CalibData_c1(76)']
            r_ld=row['CalibData_c2(76)']
            r_lu=row['CalibData_c4(76)']
            ir_ed=row['CalibData_c1(136)']
            ir_ld=row['CalibData_c2(136)']
            ir_lu=row['CalibData_c4(136)']

            try:
                # Compute Rrs for RED:650nm
                rrs650 = calc_reflectance(ed=r_ed,
                                          ld=r_ld,
                                          lu=r_lu)
                # Compute Rrs for IR:850nm
                rrs850 = calc_reflectance(ed=ir_ed,
                                          ld=ir_ld,
                                          lu=ir_lu)
                # Compute SPM
                css = css_jirau(rrs850, rrs650)

            except Exception as e:
                log.info(f'Exception: {e}')
                css = None
                rrs650 = None
                rrs850 = None

            print({'date': row['TIMESTAMP'],
                   'Ed650': r_ed,
                   'Ld650': r_ld,
                   'Lu650': r_lu,
                   'Ed850': ir_ed,
                   'Ld850': ir_ld,
                   'Lu850': ir_lu,
                   'Rrs650': rrs650,
                   'Rrs850': rrs850,
                   'css': css})
        return

    def process_csv(self, files):
        """
        Given a list of csv files, compute the RRS values over them.
        """
        total = len(files)
        for n, f in enumerate(files):
            log.info(f'Processing {n+1}/{total} ...')
            self.compute_values(f)
            log.info(f'Processing completed for: Rrs650, Rrs650 and CSS.')
        return
