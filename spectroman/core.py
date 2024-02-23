from os.path import basename
from datetime import datetime, time, timedelta

from spectroman.util import *
from spectroman.data import *
from spectroman.const import *
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
        """
        Calculate the linear interpolation.
        """
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
            df = df.join(df_out, how='left')
        finally:
            return df

    def read_csv(self, csv):
        """
        Parse csv file its data frame representation.
        """
        df = pd.DataFrame()
        try:
            df = csv_to_df(csv)
        except Exception as e:
            pass
        finally:
            return df

    def process_df(self, df):
        """
        Given a csv files, compute the interpolation and RRS values.
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
            for input_cols, output_cols in rss_param_table:
                df = self.calc_rss(df, input_cols, output_cols)
        else:
            df = pd.DataFrame()
        return df

    def insert_docs(self, path):
        """
        Process and cache the data-frames into the data base.
        """
        files = list_csvs(path)
        for f in [file_to_str_io(f) for f in files]:
            df = self.read_csv(f)
            if (not df.empty) and len(df) > 1:
                self.db.insert_docs(df.to_dict('records'), conf['DB_COLL_RAW'])
        pass

    def clean_docs(self):
        """
        Clear NaN values from the raw data there is located at
        conf['DB_COLL_RAW'].
        """
        for doc in self.db.fetch_docs(noteq_filter(calib_cols),
                                      {},
                                      conf['DB_COLL_RAW']):
            # convert timestamp string to ISODate
            try:
                doc['TIMESTAMP'] =\
                    datetime.strptime(doc['TIMESTAMP'], "%Y-%m-%d %H:%M:%S")
            except:
                pass
            else:
                self.db.insert_doc(doc, conf['DB_COLL_DF'])
            finally:
                pass

    def convert_docs(self):
        """
        Convert string types to float types.
        """
        for doc in self.db.fetch_docs(fieldstr_filter(calib_columns),
                                      {},
                                      conf['DB_COLL_DF']):
            values = {}
            for key in calib_columns:
                if (type(doc[key]) == str):
                    values[key] = float('.'.join(doc[key].split(".")[:2]))
                    # update values if necessary
            if (len(values) > 0):
                self.db.update_doc({"_id": doc['_id']},
                                   {"$set": values},
                                   conf['DB_COLL_DF'])
    def process_intp(self):
        """
        Process the linear interpolation for the database data.
        """
        for doc in self.db.fetch_docs(notexists_filter(intp_columns),
                                      {},
                                      conf['DB_COLL_DF']):
            # cache the id
            id = doc['_id']
            # remove id from the dictionary
            doc.pop('_id', None)
            # parse dict to data frame
            df = dict_to_df(doc)

            # calculate the interpolation
            for input_cols, output_cols, wl_data in intp_table:
                df = self.interpolate(df,
                                      input_cols,
                                      output_cols,
                                      wl_data)
                # compute the rss values from the interpolated parameters
            for input_cols, output_cols in rss_param_table:
                df = self.calc_rss(df, input_cols, output_cols)

            # get values from the data frame
            try:
                values = df.loc[:,\
                                ed_cols +
                                ld1_cols +
                                ld2_cols +
                                lu1_cols +
                                lu2_cols +
                                rss1_cols +
                                rss2_cols].to_dict('records')[0]
            except Exception as e:
                log.info(f"Exception {e}")
            else:
                # update the document with the new record values
                self.db.update_doc({"_id": id},
                                   {"$set": values},
                                   conf['DB_COLL_DF'])
            finally:
                pass
        pass

    def process_css(self):
        """
        Calculate the css values for the database data.
        """
        for doc in self.db.fetch_docs({}, {}, conf['DB_COLL_DF']):
            values = {}
            try:
                values['css1'] = css_jirau(doc['rss1_650'], doc['rss1_850'])
                values['css2'] = css_jirau(doc['rss2_650'], doc['rss2_850'])
            except Exception as e:
                log.info(f"Exception {e}")
            else:
                # update the document with the new record values
                self.db.update_doc({"_id": doc['_id']},
                                   {"$set": values},
                                   conf['DB_COLL_DF'])
            finally:
                pass

    def plot_basic_graph(self, date):
        """
        Plot the base graph (15 to 15 minutes), using the database values.
        """
        # parse the day range
        start = datetime.combine(date, time(6, 0, 0))
        end = datetime.combine(date, time(18, 15, 0))

        docs = [d for d in \
                self.db.fetch_docs({'TIMESTAMP': {'$gte': start,
                                                  '$lte': end}},
                                   get_intp_selection(),
                                   conf['DB_COLL_DF'])\
                .sort({'TIMESTAMP': 1})]

        # variation 15 in 15 minutes
        delta = start + timedelta(minutes=15)

        # save the list of dates and points
        dates = []
        point = []
        points = []

        # docs counter
        i = 0
        while i < len(docs):
            if (docs[i]['TIMESTAMP'] <= delta):
                point.append(docs[i])
                i += 1
            else:
                dates.append(start.strftime("%Y-%m-%d-%H-%M-%S"))
                points.append(point)
                start = delta
                delta = delta + timedelta(minutes=15)
                point = []

        # for-each list of points, plot the graph
        for i in range(len(points)):
            if (len(points[i]) >= 1):
                self.plot.base_graph(dates[i], points[i])
        pass

    def plot_daily_graph(self, date):
        """
        Plot the daily graph using the database values.
        """
        beg = datetime.combine(date, time(6, 0, 0))
        end = datetime.combine(date, time(18, 0, 0))
        docs = [d for d in \
                self.db.fetch_docs({'TIMESTAMP': {'$gte': beg,
                                                  '$lte': end}},
                                   get_daily_selection(),
                                   conf['DB_COLL_DF'])\
                .sort({'TIMESTAMP': 1})]

        if (len(docs) >= 1):
            self.plot.daily_graph(date, docs)

    def plot_monthly_graph(self):
        """
        Get the docs from the database and plot the SSS graph.
        """
        years  = [2023, 2024]
        months = [i for i in range(1, 13)]
        dates = []

        # generate months of the year
        for year in years:
            for month in months:
                dates.append(month_date_pair(year, month))
            pass
        pass

        for beg, end in dates:
            docs = []
            for doc in self.db.fetch_docs({'TIMESTAMP': {'$gte': beg,
                                                         '$lte': end}},
                                          get_css_selection(),
                                          conf['DB_COLL_DF']):
                docs.append(doc)

            if (len(docs) >= 1):
                times = [doc['TIMESTAMP'] for doc in docs]
                self.plot.monthly_css(beg, end, times, docs)
        pass

    def process_files(self, files):
        """
        Given a list of csv files, compute the RRS values over them
        and persist it using the database module.
        """
        # record start time
        n = len(files)
        gs = {}

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
                self.plot.base_graph_from_df(df)
                # update counter
            i = i + 1

        # move files
        move_csvs(files)
        pass
