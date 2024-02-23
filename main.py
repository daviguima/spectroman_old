import argparse
import importlib
import spectroman

from spectroman.log import log
from spectroman.ftp import Ftp
from spectroman.conf import conf
from spectroman.util import  *
from spectroman.core import Spectroman

def process_csvs():
    """
    Process the previous fetched csvs.
    """
    s = Spectroman()
    s.process_files(list_csvs())
    pass

def plot_basic(dates):
    """
    Plot basic graph from dates list.
    """
    s = Spectroman()
    for date in dates:
        s.plot_basic_graph(date)
        pass
    pass

def plot_daily(dates):
    """
    Plot daily graph from dates list.
    """
    s = Spectroman()
    for date in dates:
        s.plot_daily_graph(date)
    pass

def insert_data():
    """
    Insert data to the database.
    """
    s = Spectroman()
    s.insert_docs(conf['DATA_OUTPUT'])

def clean_data():
    """
    Remove inconsistent data and convert strings to float.
    """
    s = Spectroman()
    s.clean_docs()
    s.convert_docs()
    pass

def process_data():
    """
    Process database data.
    """
    s = Spectroman()
    s.process_intp()
    s.process_css()
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='SPECTROMAN - Spectral Manager for SPECTROSED '
        'Fetch data over a FTP server and implements pre and post-processing routines.')
    parser.add_argument('-c', '--csv',
                        help='Process csv files',
                        action='store_true')

    parser.add_argument('-p', '--process',
                        help='Process data',
                        action='store_true')

    parser.add_argument('-i', '--insert',
                        help='Clean inconsistent data from the database.',
                        action='store_true')

    parser.add_argument('--clean',
                        help='Clean data to the database (mongodb).',
                        action='store_true')

    parser.add_argument('-a', '--all',
                        help='Insert and process data.',
                        action='store_true')

    parser.add_argument('-f', '--fetch',
                        help='Download csv files from FTP',
                        action='store_true')

    parser.add_argument('-b', '--basic',
                        help='Plot basic graph.',
                        action='store_true')

    parser.add_argument('-d', '--day',
                        help='Plot daily graph.',
                        action='store_true')

    parser.add_argument('-m', '--month',
                        help='Plot SSS segregation monthly graph.',
                        action='store_true')

    parser.add_argument('-s', '--start',
                        help='Start date in the format: year-month-day.',
                        nargs='?')

    parser.add_argument('-e', '--end',
                        help='End date in the format: year-month-day.',
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
    elif args['insert']:
        insert_data()
    elif args['process']:
        process_data()
    elif args['clean']:
        clean_data()
    elif args['all']:
        insert_data()
        clean_data()
        process_data()
    elif args['basic']:
        plot_basic(get_dates(args['start'], args['end']))
    elif args['day']:
        plot_daily(get_dates(args['start'], args['end']))
    elif args['fetch']:
        fetch_csvs()
    else:
        pass
