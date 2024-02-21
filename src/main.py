import argparse
import importlib
from datetime import datetime

from core import Spectroman
from ftp import Ftp

from log import log
from conf import conf
from util import *

def process_csvs():
    """
    Process the previous fetched csvs.
    """
    s = Spectroman()
    s.process_files(list_csvs())
    pass

def plot_basic(dates):
    """
    """
    s = Spectroman()
    for date in dates:
        s.plot_basic_graph(date)
        pass
    pass

def plot_daily(dates):
    """
    """
    s = Spectroman()
    for date in dates:
        s.plot_daily_graph(date)
    pass

def fetch_csvs():
    """
    Fetch csvs file from the FTP host, they will be
    saved on disk at DATA_OUTPUT file set in settings.ini.
    """
    ftp = Ftp()
    ftp.connect()
    ftp.fetch_files()
    pass

def get_dates(start, end):
    "Return dates (datetimes) list."
    return \
        [d for d in daterange(datetime.strptime(start, "%Y-%m-%d"),
                              datetime.strptime(end, "%Y-%m-%d"))]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='SPECTROMAN - Spectral Manager for SPECTROSED '
        'Fetch data over a FTP server and implements pre and post-processing routines.')
    parser.add_argument('-c', '--csv',
                        help='Process csv files',
                        action='store_true')

    parser.add_argument('-f', '--fetch',
                        help='Download csv files from FTP',
                        action='store_true')

    parser.add_argument('-b', '--basic',
                        help='',
                        action='store_true')

    parser.add_argument('-d', '--day',
                        help='',
                        action='store_true')

    parser.add_argument('-m', '--month',
                        help='',
                        action='store_true')

    parser.add_argument('-s', '--start',
                        help='',
                        nargs='?')

    parser.add_argument('-e', '--end',
                        help='',
                        nargs='?')

    parser.add_argument('-v', '--version',
                        help='Displays current package version.',
                        action='store_true')

    # Converts the input arguments from Namespace() to dict
    args = parser.parse_args().__dict__

    if args['version']:
        log.info(f'SPECTROMAN version: {__version__}')
    elif args['csv']:
        process_csvs()
    elif args['basic']:
        plot_basic(get_dates(args['start'], args['end']))
    elif args['day']:
        plot_daily(get_dates(args['start'], args['end']))
    elif args['fetch']:
        fetch_csvs()
    else:
        pass
