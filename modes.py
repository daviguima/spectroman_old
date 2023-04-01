import os
import io


class Modes:
    """
    For the sake of readability any data processing should be avoided inside this class.
    Spectroman.Modes() should consume treated data from Spectroman.Core()

    Methods
    -------
    network_mode()
        Connect to the FTP server, retrieve a list of the PCD files inside it and try to
        insert them into the MongoDB COLLECTION_DF and COLLECTION_RAW defined in settings.ini.

    processing_mode()
        Connects to MongoDB and apply processing routines over its data.

    download_mode()
        Connect to the FTP server define under settings.ini:HOST, retrieve a list of the files
        inside the FTP and copy them to settings.ini:OUTPUT.

    """
    def __init__(self, super_class=None):
        # Import manager from super
        self.m = super_class

    def network_mode(self):
        """
        SPECTROMAN Default mode:
        Connect to the FTP server, retrieve a list of the PCD files inside it and try to
        insert them into the MongoDB COLLECTION_DF and COLLECTION_RAW defined in settings.ini.
        """
        manager = self.m
        ftp = manager.connect_to_ftp()  # TODO: write try-catch for exceptions.
        filenamepath = manager.ftp_get_file_list_in_path(ftp)

        total = len(filenamepath)
        manager.log.info(f'{total} total files found...')
        manager.log.info('Attempting to copy...')
        # Destination = manager.config_data['OUTPUT']  # TODO: fix destination of the copied files.

        dbname = manager.config_data['DB_NAME']
        collectionname = manager.config_data['COLLECTION_DF']
        raw_coll_name = manager.config_data['COLLECTION_RAW']
        spectral_collection, client, db = manager.get_mongo_connection()
        bkp_collection = db[raw_coll_name]
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
                    manager.log.info(f'Attempting to parse virtual file into a pd.DataFrame.')
                    virtual_file.seek(0)
                    df = manager.get_station_data_df(virtual_file)
                    manager.log.info(f'Rows found in file:{len(df)}')

                # Even if the file passed the size test, assert there is something inside the df
                if not file_fail_flag:
                    if df.empty or len(df) < 1:
                        manager.log.info(f'ERROR: insufficient rows in DataFrame, skipping insertion in DataBase.')
                        file_fail_flag = True
                    else:
                        # turn every df row into an dictionary entry and store them in a list
                        df_rows_list = df.to_dict('records')
                        # insert the list of rows in mongoDB
                        manager.log.info(f'Inserting data into {dbname}:{collectionname}.')
                        results = spectral_collection.insert_many(df_rows_list)
                        manager.log.info(f'Success.')
                        manager.log.info(results.inserted_ids)

                manager.log.info(f'Saving RAW PCD data to backup DB {dbname}:{raw_coll_name}')
                virtual_file.seek(0)
                raw_text_dict = {'file_name': fn, 'fail_status': file_fail_flag, 'raw_data': virtual_file}
                raw_insertion_results = bkp_collection.insert_one(raw_text_dict)
                manager.log.info(f'Success.')
                manager.log.info(raw_insertion_results.inserted_ids)

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

    def processing_mode(self):
        """
        SPECTROMAN Processing mode:
        Connects to MongoDB and apply processing routines over its data.
        """
        manager = self.m
        manager.log.info(f'Processing mode selected.')
        manager.log.info(f'Connects to MongoDB and apply processing routines over its data.')

        # mongodb+srv://spectroman:<password>@spectro-cluster0.1qj0y.mongodb.net/?retryWrites=true&w=majority
        pass

    def download_mode(self):
        """
        SPECTROMAN Downlaod mode:
        Connect to the FTP server define under settings.ini:HOST, retrieve a list of the files
        inside the FTP and copy them to settings.ini:OUTPUT.
        """
        manager = self.m
        ftp = manager.connect_to_ftp()
        filenamepath = manager.ftp_get_file_list_in_path(ftp)
        total = len(filenamepath)
        manager.log.info(f'{total} total files found...')
        manager.log.info('Attempting to copy...')
        for n, fn in enumerate(filenamepath):
            localcopy = manager.config_data['OUTPUT'] + os.path.basename(fn)
            print(f'Downloading {n + 1} of {total}: {fn}...')
            print(f'Saving copy at: {localcopy}')
            ftp.retrbinary('RETR ' + fn, open(localcopy, 'wb').write)
        ftp.quit()
        pass