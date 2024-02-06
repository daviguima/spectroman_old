import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as pltdates
from datetime import timedelta

from log import log
from const import *

class Plot:
    def __init__(self):
        pass

    def plot_values(self, axs, title, x, y, fmt=""):
        """
        """
        axs.set_title(title)
        axs.plot(x, y, fmt)

    def base_graph(self, dts, docs):
        """
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
                pass
        pass

        figname = 'plots/' + dts
        # save the figure
        fig.savefig(figname, dpi=200)
        # the figure was saved
        log.info(f'{figname} saved')
        # close the plot
        plt.close()

    def daily_graph(self, beg, end, date, times, docs):
        """
        """
        fig, axs = plt.subplots(6, 3, figsize=(40, 32))
        axs = axs.flat
        xfmt = pltdates.DateFormatter('%H:%M')

        for i, title in enumerate(daily_graph_dict.keys()):
            for key in daily_graph_dict[title]:
                self.plot_values(axs[i],
                                 title,
                                 times,
                                 [d[key] for d in docs],
                                 fmt='o-.')
                axs[i].grid(color='gray', linestyle='--')
                axs[i].xaxis.set_major_formatter(xfmt)
                # axs[i].set_xlim(left=beg, right=end)
            pass
        pass

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
        # figure name
        figname = 'plots/' + 'daily_' + date.strftime("%Y-%m-%d")
        # save the figure
        fig.savefig(figname, dpi=200)
        # the figure was saved
        log.info(f'{figname} saved')
        # close the plot
        plt.close()

    def monthly_css(self, beg, end, times, docs):
        """
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
                     basefmt = 'C2-',
                     bottom = 0)
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
        # figure name
        figname = 'plots/' + 'sss_' + beg.strftime("%Y-%m")
        # save the figure
        fig.savefig(figname, dpi=200)
        # the figure was saved
        log.info(f'{figname} saved')
        # close the plot
        plt.close()
        pass

    def gen_plot(self, lst, axs, title, dt):
        """
        """
        axs.set_title(title + dt)
        axs.plot(intp_arr, lst)

    def gen_fig(self, df):
        """
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
            figname = 'plots/' + datetime

            for key, title, cols in plot_table:
                df[(df['TIMESTAMP'] >= beg) &
                   (df['TIMESTAMP'] <= end)][cols].\
                   apply(self.gen_plot,
                         args=(axs[key], title, datetime),
                         raw=True,
                         axis=1)
                # save the figure
            fig.savefig(figname, dpi=200)
            # the figure was saved
            log.info(f'{figname} saved')
            # close the plot
            plt.close()
