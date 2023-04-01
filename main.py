import os
import io
import sys
import ftplib
import logging
import pymongo
import datetime
import argparse
import numpy as np
import pandas as pd

from decouple import config

version = '1.0.1'


class Spectroman:
    """Provides methods for the radiometric data manipulation using FTP """

    def __init__(self, input_arguments=None):

        self.config_data = self.get_config_dict()
        self.INSTANCE_TIME_TAG = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
        logfile = os.path.join(
            self.config_data['OUTPUT'], 'spectroman_' + self.INSTANCE_TIME_TAG + '.log')
        self.log = self.create_log_handler(logfile)
        self.log.info(f'Running Spectral Manager from: {sys.path[0]}')
        self.log.info(f'Saving LOG at: {logfile}')
        self.log.info(f'Input arguments: {input_arguments}')

    @staticmethod
    def get_config_dict():
        """
        Access the settings.ini file and return it as a dict.
        """
        setup_dict = {
            'HOST': config('HOST_ADDRESS'),
            'USER': config('USER'),
            'PASS': config('PASS'),
            'FTP_ROOT': config('FTP_ROOT'),
            'FTP_TODO': config('FTP_TODO'),
            'FTP_DONE': config('FTP_DONE'),
            'FTP_FAIL': config('FTP_FAIL'),
            'OUTPUT': config('OUTPUT'),
            'DBCON': config('DBCON'),
            'DB_NAME': config('DB_NAME'),
            'COLLECTION_NAME': config('COLLECTION_NAME')
        }

        return setup_dict

    def connect_to_ftp(self):
        self.log.info(
            f'Attempting connection to host: {self.config_data["HOST"]}')
        ftp = ftplib.FTP(host=self.config_data["HOST"])
        ftp.login(user=self.config_data["USER"],
                  passwd=self.config_data["PASS"])
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
        db_name = self.config_data['DB_NAME']
        collection_name = self.config_data['COLLECTION_NAME']
        client = pymongo.MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]
        return collection, client

    @staticmethod
    def pcdtxt2array(dir):
        # https://github.com/Tayline13/SPECTROSED/blob/main/data_cleaning.ipynb
        file_arr = np.genfromtxt(dir, delimiter = ',', skip_header = 1, dtype = 'str', invalid_raise = False)
        if (file_arr.ndim == 1):
            file_arr = np.array([], dtype = 'str')
        else:
            file_arr = file_arr[1:]
            if(len(file_arr) > 0):
                if(file_arr[0][0] == '"TS"' or file_arr[0][0] == 'TS' or file_arr[0][0] == '""' or file_arr[0][0] == ''):
                    file_arr = file_arr[1:]
                    if(len(file_arr) > 0):
                        if(file_arr[0][0] == '""' or file_arr[0][0] == ''):
                            file_arr = file_arr[1:]
        return file_arr
    
    def get_station_data_df(self, csv_file):

        # https://stackoverflow.com/questions/16108526/how-to-obtain-the-total-numbers-of-rows-from-a-csv-file-in-python
        # row_count = sum(1 for row in csv_file)
        # if row_count < 1:
        #     self.log.info(f'Insufficient number of rows in file (row count = {row_count}).')
        #     self.log.info(f'Skipping and returning empty DataFrame.')
        #     df = pd.DataFrame()
        #     return df
        go_on = True
        self.log.info(f'Retrieving column names for file: {self.fn}')
        try:
            # get only the column names from the file
            colnames = np.genfromtxt(csv_file, delimiter = ',', dtype = 'str', invalid_raise=False, skip_header=1, max_rows=1)
            self.log.info(f'Number of columns founds in file = {len(colnames)}.')
            # Once read the pointer needs to return to the head of the _io.BytesIO object
            csv_file.seek(0)

        except Exception as e:
            self.log.info(f'Broken file header')
            self.log.info(f'ERROR: {e}')
            self.log.info(f'Returning empty DataFrame.')
            go_on = False

        if go_on:
            # Try to use the colnames found in the file to capture the file content.
            try:
                pcd_rows = self.pcdtxt2array(csv_file)
                self.log.info(f'Number of lines founds in file = {len(pcd_rows)}.')
                self.log.info(f'Number of columns founds in array = {len(pcd_rows[0])}.')
                self.log.info(f'Generating pd.DataFrame from array data.')
                df = pd.DataFrame(pcd_rows, columns=colnames)

            except Exception as e:
                self.log.info(f'ERROR: {e}')
                self.log.info(f'Returning empty DataFrame.')
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()

        return df

    @staticmethod
    def create_log_handler(fname):
        # based in these answers:
        # https://stackoverflow.com/questions/62835466/create-a-separate-logger-for-each-process-when-using-concurrent-futures-processp
        # https://stackoverflow.com/questions/25454517/level-and-formatting-for-logging-streamhandler-changes-after-running-subproces
        logger = logging.getLogger(fname)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(message)s', datefmt='%H:%M:%S')
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


def net_mode(in_args=None):
    """
    SPECTROMAN Default mode:
    given a settings.ini configuration file, connect to a FTP server, retrieve a
    list of the files inside the FTP server, and try to insert them into a MongoDB
    specified inside the same settings.ini file.
    """
    ftp = manager.connect_to_ftp()  # TODO: write try-catch for exceptions.
    filenamepath = manager.ftp_get_file_list_in_path(ftp)

    total = len(filenamepath)
    manager.log.info(f'{total} total files found...')
    manager.log.info('Attempting to copy...')
    # Destination = manager.config_data['OUTPUT']  # TODO: fix destination of the copied files.

    spectral_collection, client = manager.get_mongo_connection()
    try:
        for n, fn in enumerate(filenamepath):
            file_fail_flag = False
            # localcopy = Path(Destination + os.path.basename(fn))
            manager.log.info(f'Processing {n + 1} of {total}: {fn}...')
            manager.fn = fn
            # manager.log.info(f'Saving copy at: {localcopy}')
            # ftp.retrbinary('RETR ' + fn, open(localcopy, 'wb').write)
            manager.log.info(f'Creating empty dummy file locally.')
            # https://stackoverflow.com/questions/48815110/read-a-csv-file-stored-in-a-ftp-in-python
            virtual_file = io.BytesIO()
            manager.log.info(f'Downloading remote file content to local dummy copy...')
            ftp.retrbinary("RETR {}".format(fn), virtual_file.write)
            manager.log.info(f'Download completed.')
            # after writing: go back to the start of the virtual file
            virtual_file.seek(0)

            # Test for empty files
            fsize = virtual_file.getbuffer().nbytes
            if fsize < 1000:
                file_fail_flag = True
            else:
                manager.log.info(
                    f'Attempting to parse virtual file into a pd.DataFrame.')
                virtual_file.seek(0)
                df = manager.get_station_data_df(virtual_file)
                manager.log.info(f'Rows found in file:{len(df)}')

            # If the file passed the size test, check if the output df has something inside.
            if not file_fail_flag:
                if df.empty or len(df) < 1:
                    manager.log.info(
                        f'ERROR: insufficient rows in DataFrame, skipping insertion in mongoDB.')
                    file_fail_flag = True
                else:
                    # turn every df row into an dictionary entry and store them in a list
                    df_rows_list = df.to_dict('records')
                    # insert the list of rows in mongoDB
                    dbname = manager.config_data['DB_NAME']
                    collectionname = manager.config_data['COLLECTION_NAME']
                    manager.log.info(
                        f'Inserting data into {dbname}:{collectionname}.')
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

    except Exception as e:
        manager.log.info(e)
        client.close()

    finally:
        client.close()
        ftp.quit()


def pro_mode(in_args=None):
    """
    SPECTROMAN Processing mode:
    No network tasks will be performed. Connects to a MongoDB given the setting.ini
    configuration file and apply pre-defined processing routines over its data.
    """
    manager.log.info(f'Processing mode selected.')
    manager.log.info(f'No network tasks will be performed. Connects to a MongoDB given the setting.ini'
                     f' configuration file and apply pre-defined processing routines over its data.')
    pass


def dl_mode(in_args=None):
    """
    SPECTROMAN Downlaod mode:
    """
    ftp = manager.connect_to_ftp()
    filenamepath = manager.ftp_get_file_list_in_path(ftp)

    total = len(filenamepath)
    manager.log.info(f'{total} total files found...')
    manager.log.info('Attempting to copy...')
    for n, fn in enumerate(filenamepath):
        localcopy = manager.config_data['OUTPUT'] + os.path.basename(fn)
        print(f'Downloading {n+1} of {total}: {fn}...')
        print(f'Saving copy at: {localcopy}')
        ftp.retrbinary('RETR ' + fn, open(localcopy, 'wb').write)
    ftp.quit()
    pass


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='SPECTROMAN - Spectral Manager for SPECTROSED '
                    'Fetch data over a FTP server and implements pre and post-processing routines.')
    parser.add_argument("-n", "--network", help="Run SPECTROMAN over a FTP and feed the collected data to a DB",
                        action='store_true')
    parser.add_argument('-p', '--process',
                        help='Run in processing mode', action='store_true')
    parser.add_argument(
        '-l', '--local', help='Run in local download mode', action='store_true')
    parser.add_argument(
        '-v', '--version', help='Displays current package version.', action='store_true')
    # Converts the input arguments from Namespace() to dict
    args = parser.parse_args().__dict__
    # Instantiate a Spectroman class object in order to exploit its logging capabilities
    manager = Spectroman(input_arguments=args)

    if args['version']:
        manager.log.info(f'SPECTROMAN version: {version}')
    elif args['network']:
        net_mode(in_args=args)
    elif args['process']:
        pro_mode(in_args=args)
    elif args['local']:
        dl_mode(in_args=args)
