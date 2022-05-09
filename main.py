import os
import io
import sys
import ftplib
import logging
import datetime
import pymongo
import pandas as pd

from pathlib import Path
from decouple import config


class Spectroman:
    """Provides methods for the radiometric data manipulation using FTP """

    def __init__(self):
        self.config_data = self.get_config_dict()
        self.INSTANCE_TIME_TAG = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
        logfile = os.path.join(self.config_data['OUTPUT'], 'spectroman_' + self.INSTANCE_TIME_TAG + '.log')
        self.log = self.create_log_handler(logfile)
        self.log.info(f'Running Spectral Manager from: {sys.path[0]}')
        self.log.info(f'Saving LOG at: {logfile}')

    @staticmethod
    def get_config_dict():
        """Access the settings.ini file and return it as a dict"""
        setup_dict = {
            'HOST': config('HOST_ADDRESS'),
            'USER': config('USER'),
            'PASS': config('PASS'),
            'FTP_ROOT': config('FTP_ROOT'),
            'FTP_TODO': config('FTP_TODO'),
            'FTP_DONE': config('FTP_DONE'),
            'FTP_FAIL': config('FTP_FAIL'),
            'OUTPUT': config('OUTPUT_PATH'),
            'DBCON': config('CONN_STR')
        }
        return setup_dict

    def connect_to_ftp(self):
        self.log.info(f'Attempting connection to host: {self.config_data["HOST"]}')
        ftp = ftplib.FTP(host=self.config_data["HOST"])
        ftp.login(user=self.config_data["USER"], passwd=self.config_data["PASS"])
        self.log.info('Succes.')
        return ftp

    def ftp_get_file_list_in_path(self, ftp, path=None):
        """Given an FTP connection and optionally a destination, return a list of files in that path."""
        # Test if the user wants to navigate to a different path other than the one inside the config file
        if path:
            user_path = path
        else:
            user_path = self.config_data["FTP_ROOT"]

        self.log.info(f'Walking to: {user_path}')
        ftp.cwd(user_path)
        self.log.info('Retrieving list of files...')
        remote_fnames = ftp.nlst()  # get filenames within the directory
        if ".." in remote_fnames:
            remote_fnames.remove("..")
        if "." in remote_fnames:
            remote_fnames.remove(".")
        return remote_fnames

    def get_mongo_connection(self):
        connection_string = self.config_data['DBCON']
        client = pymongo.MongoClient(connection_string)
        db = client.db_spectro
        collection = db.spectral_collection
        return collection

    def get_station_data_df(self, csv_file):
        # get only the column names from the file
        colnames = list(pd.read_csv(csv_file, skiprows=1, on_bad_lines='warn', nrows=1).columns)
        csv_file.seek(0)  # Once read the pointer needs to return to the head of the _io.BytesIO object
        try:
            df = pd.read_csv(csv_file,
                             names=colnames,
                             skiprows=4,
                             index_col=False,
                             na_values="-99",
                             parse_dates=['TIMESTAMP'],
                             infer_datetime_format=True,
                             on_bad_lines='warn')
        except pd.errors.ParserError as e:
            self.log.info(f'ERROR: {e}')
            self.log.info(f'Returning empty DataFrame.')
            df = pd.DataFrame()

        return df

    @staticmethod
    def create_log_handler(fname):
        # based in these answers:
        # https://stackoverflow.com/questions/62835466/create-a-separate-logger-for-each-process-when-using-concurrent-futures-processp
        # https://stackoverflow.com/questions/25454517/level-and-formatting-for-logging-streamhandler-changes-after-running-subproces
        logger = logging.getLogger(fname)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
        # Output log to file
        fileHandler = logging.FileHandler(fname)
        fileHandler.setLevel(logging.INFO)
        fileHandler.setFormatter(formatter)
        # Output log to console
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatter)
        # Use both handlers
        logger.addHandler(fileHandler)
        logger.addHandler(console)
        return logger


def main():
    """Entrypoint"""
    manager = Spectroman()
    ftp = manager.connect_to_ftp()
    filenamepath = manager.ftp_get_file_list_in_path(ftp)

    total = len(filenamepath)
    manager.log.info(f'{total} total files found...')
    manager.log.info('Attempting to copy...')
    # Destination = manager.config_data['OUTPUT']

    spectral_collection = manager.get_mongo_connection()

    for n, fn in enumerate(filenamepath):
        file_fail_flag = False
        # localcopy = Path(Destination + os.path.basename(fn))
        manager.log.info(f'Processing {n + 1} of {total}: {fn}...')
        # manager.log.info(f'Saving copy at: {localcopy}')
        # ftp.retrbinary('RETR ' + fn, open(localcopy, 'wb').write)
        manager.log.info(f'Creating empty dummy file locally.')
        # https://stackoverflow.com/questions/48815110/read-a-csv-file-stored-in-a-ftp-in-python
        virtual_file = io.BytesIO()
        manager.log.info(f'Downloading remote file content to local dummy copy...')
        ftp.retrbinary("RETR {}".format(fn), virtual_file.write)
        # after writing: go back to the start of the virtual file
        manager.log.info(f'Download completed.')
        # after writing: go back to the start of the virtual file
        virtual_file.seek(0)

        # Test for empty files
        fsize = virtual_file.getbuffer().nbytes
        if fsize < 1000:
            file_fail_flag = True
        else:
            manager.log.info(f'Attempting to parse virtual file into a pd.DataFrame.')
            virtual_file.seek(0)
            df = manager.get_station_data_df(virtual_file)
            manager.log.info(f'Rows found in file:{len(df)}')

        # If the file passed the size test, check if the output df has something inside.
        if not file_fail_flag:
            if df.empty or len(df) < 1:
                manager.log.info(f'ERROR: insufficient rows in DataFrame, skipping insertion in mongoDB.')
                file_fail_flag = True
            else:
                # turn every df row into an dictionary entry and store them in a list
                df_rows_list = df.to_dict('records')
                # insert the list of rows in mongoDB
                manager.log.info(f'Inserting data into mongoDB spectral_collection.')
                results = spectral_collection.insert_many(df_rows_list)
                manager.log.info(f'Success.')
                manager.log.info(results.inserted_ids)

        manager.log.info(f'Parsing completed, closing virtual file.')
        virtual_file.close()

        if file_fail_flag:
            target = manager.config_data['FTP_FAIL'] + os.path.basename(fn)
        else:
            target = manager.config_data['FTP_DONE'] + os.path.basename(fn)

        manager.log.info(f'Moving file {fn} to {target}.')
        ftp.rename(fn, target)
        manager.log.info(f'Done.')

    ftp.quit()


if __name__ == "__main__":
    main()
