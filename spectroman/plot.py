import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as pltdates
from datetime import timedelta

from spectroman.log import log
from spectroman.const import *
from spectroman.conf import conf

class Plot:
    def __init__(self):
        pass

    def plot_values(self, axs, title, x, y, fmt=""):
        """
        Plot x, y values and set title using the specific axes (axs).
        """
        axs.set_title(title)
        axs.plot(x, y, fmt)

    def save_fig(self, fig, fname):
        """
        Save figure using the figure object and its file name.
        """
        fig.savefig(fname, dpi=200)
        log.info(f'{fname} saved')
        plt.close()
        pass

    def base_graph(self, dts, docs):
        """
        Plot the base graph (15 in 15 minutes) from the selected documents.
        """
        fig = plt.figure(figsize=(7, 7), layout='constrained')
        axs = fig.subplot_mosaic([['ed', 'ed'],
                                  ["lu1", "lu2"],
                                  ["ld1", "ld2"],
                                  ["rss1", "rss2"]])
        for doc in docs:
            for key, title, cols in base_graph_table:
                self.plot_values(axs[key],
                                 title + dts,
                                 intp_arr,
                                 [doc[k] for k in cols])
        # save figure
        self.save_fig(fig, 'spectroman/plots/' + dts)
        pass

    def daily_graph(self, beg, end, date, times, docs):
        """
        Plot the daily graph from the selected documents.
        """
        fig, axs = plt.subplots(6, 3, figsize=(40, 32))
        axs = axs.flat
        xfmt = pltdates.DateFormatter('%H:%M')
        for i, title in enumerate(daily_graph_dict.keys()):

            # set y limit (if any)
            if (daily_graph_dict[title]['ylim'][1] != None and
                daily_graph_dict[title]['ylim'][0] != None):
                axs[i].set_ylim(top=daily_graph_dict[title]['ylim'][1],
                                bottom=daily_graph_dict[title]['ylim'][0])

            # set grid and x label format
            axs[i].grid(color='gray', linestyle='--')
            axs[i].xaxis.set_major_formatter(xfmt)

            # plot values using the right columns
            for key in daily_graph_dict[title]['cols']:
                self.plot_values(axs[i],
                                 title,
                                 times,
                                 [d[key] for d in docs],
                                 fmt='o-.')

        # adjust the subplots
        plt.subplots_adjust(left=0.05,
                            bottom=0.05,
                            right=0.95,
                            top=0.9,
                            wspace=0.2,
                            hspace=0.2)
        # adjust title
        plt.suptitle(f'{date}', fontsize=50, y=0.96)
        # remove axs
        fig.delaxes(axs[16])
        fig.delaxes(axs[17])
        # save figure
        self.save_fig(fig, conf['PLOT_OUTPUT'] + 'daily_' + date.strftime("%Y-%m-%d"))
        pass

    def monthly_css(self, beg, end, times, docs):
        """
        Plot the month graph (css) from the selected documents.
        """
        fig, axs = plt.subplots(1, 1, figsize=(40, 32))
        keys = monthly_graph_dict['keys']
        xfmt = pltdates.DateFormatter('%d/%m')
        axs.xaxis.set_major_formatter(xfmt)
        axs.grid(color='gray', linestyle='--')
        axs.set_ylim(top=2000)
        plt.xticks(fontsize=30)
        plt.yticks(fontsize=30)
        for key in keys[:1]:
            axs.stem(times,
                     [d[key] for d in docs],
                     linefmt = "-",
                     basefmt = 'C1-',
                     bottom=2000)
            pass

        date = beg.strftime("%m/%Y")
        endl = '\n'
        plt.suptitle(f'{date}{endl}sss mg/L', fontsize=50, y=0.96)
        plt.subplots_adjust(left=0.05,
                            bottom=0.05,
                            right=0.95,
                            top=0.9,
                            wspace=0.2,
                            hspace=0.2)
        # save the figure
        self.save_fig(fig, conf['PLOT_OUTPUT'] + 'sss_' + beg.strftime("%Y-%m"))
        pass

    def plot_lst_values(self, lst, axs, title, dt):
        """
        Plot the x, y values and set title using the specific axes (axs).
        """
        axs.set_title(title + dt)
        axs.plot(intp_arr, lst)

    def base_graph_from_df(self, df):
        """
        Plot base graph using the data frame (df) values.
        """
        day = df['TIMESTAMP'].iloc[0].strftime("%Y-%m-%d")
        beg = day + " 06:00:00"
        end = day + " 18:00:00"
        dts = pd.date_range(beg,
                            end,
                            freq='15T',
                            inclusive='both').values
        ts = []
        for i in range(0, len(dts) - 1):
            tmp = df.loc[(df['TIMESTAMP'] >= dts[i]) &
                         (df['TIMESTAMP'] <= dts[i + 1])]['TIMESTAMP']
            if not tmp.empty:
                ts.append(tmp)

        for t in ts:
            beg = t.iloc[0]
            end = t.iloc[-1]
            fig = plt.figure(figsize=(7, 7),
                             layout='constrained')

            axs = fig.subplot_mosaic([['ed', 'ed'],
                                      ["lu1", "lu2"],
                                      ["ld1", "ld2"],
                                      ["rss1", "rss2"]])

            datetime = pd.to_datetime(str(beg)).strftime("%Y-%m-%d-%H-%M-%S")

            for key, title, cols in plot_table:
                df[(df['TIMESTAMP'] >= beg) &
                   (df['TIMESTAMP'] <= end)][cols].\
                   apply(self.plot_lst_values,
                         args=(axs[key], title, datetime),
                         raw=True,
                         axis=1)
                # save the figure
            self.save_fig(fig, conf['PLOT_OUTPUT'] + datetime)
            pass
