import argparse
import importlib
import spectroman

from spectroman.core import Spectroman
from spectroman.ftp import Ftp

from spectroman.log import log
from spectroman.conf import conf
from spectroman.util import list_csvs


def process_csvs():
    """
    Process the previous fetched csvs.
    """
    s = Spectroman()
    s.db.connect()
    s.process_files(list_csvs())
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

    parser.add_argument('-p', '--process',
                        help='Process csv files',
                        action='store_true')

    parser.add_argument('-l', '--local',
                        help='Download csv files from FTP',
                        action='store_true')

    parser.add_argument('-v', '--version',
                        help='Displays current package version.',
                        action='store_true')

    # Converts the input arguments from Namespace() to dict
    args = parser.parse_args().__dict__

    if args['version']:
        log.info(f'SPECTROMAN version: {spectroman.__version__}')
    elif args['process']:
        process_csvs()
    elif args['local']:
        fetch_csvs()
