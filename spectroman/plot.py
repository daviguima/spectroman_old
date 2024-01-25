import pandas as pd
import matplotlib.pyplot as plt

from spectroman.log import log
# from const import *

class Plot:
    def __init__(self):
        pass

    def gen_plot(self, lst, axs, title, ts):
        axs.set_title(title + ts)
        axs.plot(intp_arr, lst)

    def gen_fig(self, df):
        day = df['TIMESTAMP'].iloc[0].strftime("%Y-%m-%d")
        beg = day + " 06:00:00"
        end = day + " 18:00:00"
        dts = pd.date_range(beg,
                            end,
                            freq='15T',
                            inclusive='both').values

        for i in range(0, len(dts) - 1):
            fig = plt.figure(figsize=(7, 7),
                             layout='constrained')

            axs = fig.subplot_mosaic([['ed', 'ed'],
                                      ["lu1", "lu2"],
                                      ["ld1", "ld2"],
                                      ["rss1", "rss2"]])

            # transform time stamp into the string format
            ts = pd.to_datetime(str(dts[i])).strftime("%Y-%m-%d-%H-%M-%S")
            flag = False
            # for each key, title and cols from plot table do:
            for key, title, cols in plot_table:
                # select only the right interval
                df_tmp = df[cols].loc[(df['TIMESTAMP'] >= dts[i]) &
                                      (df['TIMESTAMP'] <= dts[i + 1])]
                if not df_tmp.empty:
                    # plot the values into the axis
                    df_tmp.apply(self.gen_plot,
                                 args=(axs[key], title, ts),
                                 raw=True,
                                 axis=1)
                    flag = True
            if flag:
                fname = 'plots/' + ts
                # show the figure file name
                log.info(f'{fname} saved')
                # save the figure
                fig.savefig(fname , dpi=200)
            # clear figure
            # plt.clf()
            # close the plot
            plt.close()

