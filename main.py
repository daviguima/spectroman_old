import os
import ftplib
from pathlib import Path
from decouple import config


def main():
    print('Running Spectral Manager')
    print('Retrieving settings from settings.ini file.')

    HOST = config('HOST_ADDRESS')
    USER = config('USER')
    PASS = config('PASS')
    USER_PATH = config('USER_PATH')
    Destination = 'D:/git-repos/spectroman/retrieved/'

    print(f'Attempting connection to host: {HOST}')
    # print(type(HOST), USER, PASS, USER_PATH)
    ftp = ftplib.FTP(host=HOST)
    ftp.login(user=USER, passwd=PASS)
    print('Succes.')

    print(f'Walking to: {USER_PATH}')
    ftp.cwd(USER_PATH)

    print('Retrieving list of files...')
    filenamepath = ftp.nlst()  # get filenames within the directory

    total = len(filenamepath)
    print(f'{total} total files found...')
    print('Attempting to copy...')
    for n, fn in enumerate(filenamepath):
        localcopy = Path(Destination + os.path.basename(fn))
        print(f'Processing {n+1} of {total}: {fn}...')
        print(f'Saving copy at: {localcopy}')
        ftp.retrbinary('RETR ' + fn, open(localcopy, 'wb').write)

    ftp.quit()


if __name__ == "__main__":
    main()
