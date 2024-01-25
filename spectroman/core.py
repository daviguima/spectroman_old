import time
from os.path import basename

from spectroman.util import *
from spectroman.data import *
# from const import *
from spectroman.model import *

from spectroman.db import Db
from spectroman.ftp import Ftp
from spectroman.log import log
from spectroman.plot import Plot

class Spectroman:
    def __init__(self):
        self.db = Db()
        self.ftp = Ftp()
        self.plot = Plot()
        pass

    def interpolate(self, df, input_cols, output_cols, intp_data):
        # calculate the interpolation for param: [c1, c2, c3, c4, c4]
        try:
            df_out = df[input_cols].apply(linear_intp,
                                          args=(intp_data, intp_set),
                                          # raw=True,
                                          result_type='expand',
                                          axis=1)
            df_out.columns = output_cols
        except Exception as e:
            log.info(e)
            pass
        else:
            # df = pd.concat([df, df_intp], axis=1)
            df = df.join(df_out, how='left')
        finally:
            return df

    def calc_rss(self, df, input_cols, output_cols):
        """
        Given a data frame, compute the RRS values and
        return it.
        """
        try:
            df_out = df[input_cols].apply(calc_rrs_reflectance,
                                          # raw=True,
                                          result_type='expand',
                                          axis=1)
            df_out.columns = output_cols
        except Exception as e:
            log.info(e)
            pass
        else:
            # df = pd.concat([df, df_rss], axis=1)
            df = df.join(df_out, how='left')
        finally:
            return df

    def insert_df(self, df, coll):
        if (df != None) and (not df.empty) and len(df) > 1:
            self.db.insert(df.to_dict('list'), coll)
        pass

    def read_csv(self, csv):
        df = pd.DataFrame()
        try:
            df = csv_to_df(csv)
        except Exception as e:
            pass
        finally:
            return df

    def process_df(self, df):
        """
        Given a csv files, compute the interpolation and RRS values over them
        and persist it using the database module.
        """
        df = process_df(df)
        bk = df
        if (not df.empty) and len(df) > 1:
            # compute the interpolation
            for input_cols, output_cols, intp_data in intp_table:
                df = self.interpolate(df,
                                      input_cols,
                                      output_cols,
                                      intp_data)

            # compute the rss values from the interpolated parameters
            for input_cols, output_cols in rss_table:
                df = self.calc_rss(df, input_cols, output_cols)
        else:
            df = pd.DataFrame()
        return df

    def process_files(self, files):
        """
        Given a list of csv files, compute the RRS values over them
        and persist it using the database module.
        """
        # record start time
        n = len(files)
        gs = {}

        time_start  = time.perf_counter()
        for f in files:
            ts = basename(f).split('_')[0]
            gs.setdefault(ts, []).append(file_to_str_io(f))

        i = 0
        for k in list(gs):
            log.info(f'Processing: {k}: {i + 1}/{len(gs)}')
            df = pd.DataFrame()
            for f in gs[k]:
                tmp = self.read_csv(f)
                if not tmp.empty and len(tmp) > 1:
                    df = pd.concat([df, self.process_df(tmp)],
                                   axis=0,
                                   ignore_index=True,
                                   sort=False,
                                   copy=False)
                pass
            # generate the rss of the day
            if not df.empty and len(df) > 1:
                self.plot.gen_fig(df)
                # update counter
            i = i + 1
        pass
        # record end time
        time_end = time.perf_counter()
        # calculate the duration
        time_duration = time_end - time_start
        # report the duration
        print(f'Took {time_duration:.3f} seconds')

        pass
