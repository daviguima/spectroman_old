import os
import sys
import ftplib
import logging
import pymongo
import datetime
import numpy as np
import pandas as pd
from decouple import config  # python-decouple
from datetime import datetime
from modes import Modes


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
        'COLLECTION_DF': config('COLLECTION_DF'),
        'COLLECTION_RAW': config('COLLECTION_RAW'),
        'COLLECTION_MAIN': config('COLLECTION_MAIN')
    }
    return setup_dict


class Spectroman:
    """
    SspectroMan core class, provides methods for data manipulation
    using MongoDB and FTP connections imported from the settings.ini file

    Methods
    -------
    connect_to_ftp(self)
        Pass.

    ftp_get_file_list_in_path(self, ftp, path=None, sort_recent=True)
        Given an FTP connection and optionally a destination, return a list of files in that path.

    get_mongo_connection(self, conn2coll=None)
        Pass.

    pcd2array(pcd_file)
        Pass.

    get_station_data_df(self, csv_file)
        Pass.

    create_log_handler(fname)
        Pass.
    """

    def __init__(self, input_arguments=None):
        self.config_data = get_config_dict()
        self.INSTANCE_TIME_TAG = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
        if os.path.exists(self.config_data['OUTPUT']):
            logfile = os.path.join(
                self.config_data['OUTPUT'], 'spectroman_' + self.INSTANCE_TIME_TAG + '.log')
        else:
            print(f'Warning: OUTPUT path not found inside settings.ini!')
            logfile = os.path.join(os.getcwd(), 'spectroman_' + self.INSTANCE_TIME_TAG + '.log')

        self.log = self.create_log_handler(logfile)
        self.log.info(f'Running Spectral Manager from: {sys.path[0]}')
        self.log.info(f'Saving LOG at: {logfile}')
        self.log.info(f'Input arguments: {input_arguments}')
        self.option = Modes(self)

    def connect_to_ftp(self):
        self.log.info(
            f'Attempting connection to host: {self.config_data["HOST"]}')
        ftp = ftplib.FTP(host=self.config_data["HOST"])
        ftp.login(user=self.config_data["USER"],
                  passwd=self.config_data["PASS"])
        self.log.info('Succes.')
        return ftp

    def ftp_get_file_list_in_path(self, ftp, path=None, sort_recent=True):
        """
        Given an FTP connection and optionally a destination, return a list of files in that path.
        """
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
        if sort_recent:
            remote_fnames = sorted(remote_fnames,
                                   key=lambda s: datetime.strptime(s.split('.')[0], '%Y-%m-%d_%H-%M-%S'),
                                   reverse=True)
        return remote_fnames

    def get_mongo_connection(self, conn2coll=None):
        connection_string = self.config_data['DBCON']
        db_name = self.config_data['DB_NAME']
        if conn2coll:
            collection_name = conn2coll
        else:
            collection_name = self.config_data['COLLECTION_DF']
        client = pymongo.MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]
        return collection, client, db

    @staticmethod
    def pcd2array(pcd_file):
        file_arr = np.genfromtxt(pcd_file, delimiter=',', skip_header=1, dtype='str', invalid_raise=False)
        if file_arr.ndim == 1:
            file_arr = np.array([], dtype='str')
        else:
            file_arr = file_arr[1:]
            if len(file_arr) > 0:
                if file_arr[0][0] == '"TS"' or file_arr[0][0] == 'TS' or file_arr[0][0] == '""' or file_arr[0][0] == '':
                    file_arr = file_arr[1:]
                    if len(file_arr) > 0:
                        if file_arr[0][0] == '""' or file_arr[0][0] == '':
                            file_arr = file_arr[1:]
        return file_arr

    def get_station_data_df(self, csv_file):
        go_on = True
        try:
            # get only the column names from the file
            colnames = np.genfromtxt(csv_file,
                                     delimiter=',',
                                     dtype='str',
                                     invalid_raise=False,
                                     skip_header=1,
                                     max_rows=1)
            self.log.info(f'Number of columns founds in file = {len(colnames)}.')
            # Once read the pointer needs to return to the head of the _io.BytesIO object
            csv_file.seek(0)

        except Exception as e:
            self.log.info(f'Broken file header')
            self.log.info(f'ERROR: {e}')
            self.log.info(f'Returning empty DataFrame.')
            colnames = []
            go_on = False

        if go_on:
            # Try to use the colnames found in the file to capture the file content.
            try:
                pcd_rows = self.pcd2array(pcd_file=csv_file)
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

    @staticmethod
    def get_rrs():
        pass
