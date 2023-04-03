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

        dbname = manager.config_data['DB_NAME']
        collectionname = manager.config_data['COLLECTION_DF']
        raw_coll_name = manager.config_data['COLLECTION_RAW']
        spectral_collection, client, db = manager.get_mongo_connection()
        bkp_collection = db[raw_coll_name]
        try:
            for n, fn in enumerate(filenamepath):
                file_fail_flag = False
                manager.log.info(f'Processing {n + 1} of {total}: {fn}...')
                manager.fn = fn
                manager.log.info(f'Creating empty dummy file locally.')
                # https://stackoverflow.com/questions/48815110/read-a-csv-file-stored-in-a-ftp-in-python
                virtual_file = io.BytesIO()
                manager.log.info(f'Downloading remote file content to local dummy copy...')
                try:
                    # Counter-measure for when the PCD's CRBOX is writing to the same file in the FTP:
                    # 550 The process cannot access the file because it is being used by another process.
                    ftp.retrbinary("RETR {}".format(fn), virtual_file.write)
                except Exception as e:
                    manager.log.info(f'Error: {e}')
                    manager.log.info(f'Skipping to next file.')
                    continue

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

                manager.log.info(f'Saving RAW PCD data to backup {dbname}:{raw_coll_name}')
                virtual_file.seek(0)
                raw_text_dict = {'file_name': fn, 'fail_status': file_fail_flag, 'raw_data': virtual_file.getvalue()}
                raw_insertion_results = bkp_collection.insert_one(raw_text_dict)
                manager.log.info(f'Success.')
                manager.log.info(raw_insertion_results.inserted_id)

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
        Connects to MongoDB and apply processing routines over the its data.
        """
        manager = self.m 
        manager.log.info(f'Processing mode selected.')
        manager.log.info(f'Connects to MongoDB and apply processing routines over its data.')

        # Connect and retrieve files inside collection jirau_df
        spectral_collection, _, _ = manager.get_mongo_connection(collection=manager.config_data['COLLECTION_DF'])
        main_collection, _, _ = manager.get_mongo_connection(collection=manager.config_data['COLLECTION_MAIN'])
        
        query = { "$or": [ { "processed": False }, { "processed": { "$exists": False } } ] }

        # Find all documents matching the query and store them in a list
        manager.log.info(f'Retrieving unprocessed data from Database...')
        mdb_query_data = list(spectral_collection.find(query))
        
        total = len(mdb_query_data)
        manager.log.info(f'Done : {total} entries where found.')

        manager.log.info(f'Computing Rrs over data for 650 and 850nm...')
        processed_queries = manager.update_query2rrs(query_list=mdb_query_data)

        # Update each document with a new 'result' field, a 'date' field, and set 'processed' to True
        for n, document in enumerate(processed_queries):
            try:
                # Keep track of the entries that are already processed
                manager.log.info(f'Updating {n+1} / {total} ...')
                update = { "$set": { "date": document['date'], "processed": True } }
                spectral_collection.update_one({ "_id": document['_id'] }, update)

                # Add the new fields to collection jirau_main
                main_collection.insert_one(document)
            except Exception as e:
                manager.log.info(f"Exception {e}")
                manager.log.info(f"Skipping entry {document['_id']}")
        
        manager.log.info(f"Processing mode completed.")
        pass

    def update_atlas(self):
        """
        SPECTROMAN Update Atlas Dashboard mode:
        Fetch the entries in Collection:MAIN and connects to the Atlas cloud instance
        to replicate the content for both collections in order to feed the dashboard.
        """
        manager = self.m 
        manager.log.info(f'Atlas collection update mode selected.')
        # Connect and retrieve files inside collection jirau_df
        main_collection, _, _ = manager.get_mongo_connection(collection=manager.config_data['COLLECTION_MAIN'])
        
        query = { "$or": [ { "on_cloud": False }, { "on_cloud": { "$exists": False } } ] }
        # Find all documents matching the query and store them in a list
        manager.log.info(f'Retrieving unprocessed data from Database...')
        mdb_query_data = list(main_collection.find(query))
        
        total = len(mdb_query_data)
        manager.log.info(f'Done : {total} entries where found.')

        # open connection to Atlas
        atlas_db_name = manager.config_data['DB_ATLAS']
        atlas_coll_name = manager.config_data['COLLECTION_ATLAS']
        atlas_collection, _, _ = manager.get_mongo_connection(conn_str=manager.config_data['DBCON_ATLAS'],
                                                              db_name=atlas_db_name,
                                                              collection=atlas_coll_name)
        
        manager.log.info(f'Connecting to cloud Atlas DB: {atlas_db_name} using collection: {atlas_coll_name}')
        # Update each document with a new 'result' field, a 'date' field, and set 'processed' to True
        for n, document in enumerate(mdb_query_data):
            try:
                # Keep track of the entries that are already processed
                manager.log.info(f'Updating {n+1} / {total} ...')
                # Copy entry to remote Atlas collection
                atlas_collection.insert_one(document)
                # Mark the local entry to avoid reprocessing in the future
                update = { "$set": { "on_cloud": True } }
                main_collection.update_one({ "_id": document['_id'] }, update)

            except Exception as e:
                manager.log.info(f"Exception {e}")
                manager.log.info(f"Skipping entry {document['_id']}")
        
        manager.log.info(f"Atlas update completed.")
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
            localcopy = manager.config_data['LOG_OUTPUT'] + os.path.basename(fn)
            print(f'Downloading {n + 1} of {total}: {fn}...')
            print(f'Saving copy at: {localcopy}')
            ftp.retrbinary('RETR ' + fn, open(localcopy, 'wb').write)
        ftp.quit()
        pass