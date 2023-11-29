import ftplib
from datetime import datetime
from conf import conf
from log import log

class Ftp:
    def __init__(self, host=None, path=None, user=None, passwd=None):
        self.ftp = None
        self.host = host or conf['FTP_HOST']
        self.user = user or conf['FTP_USER']
        self.path = path or conf['FTP_PATH']
        self.passwd = passwd or conf['FTP_PASS']

    def connect(self):
        self.ftp = ftplib.FTP(host=self.host, timeout=60)
        # ftp login
        self.ftp.login(user=self.user,
                       passwd=self.passwd)
        # return void
        return

    def files_list(self, path=None, sort_recent=True):
        path = path or self.path
        files = []
        # change current directory
        self.ftp.cwd(path)
        # convert file string to file list, remove the last element
        self.ftp.retrlines('NLST', lambda s : files.append(s))
        # if sort recent is set to false return files
        if sort_recent:
            files.sort(key=lambda s: datetime.strptime(s.split('.')[0],
                                                       '%Y-%m-%d_%H-%M-%S'))
        return files

    def fetch_files(self, path=None):
        files = self.files_list(path)
        total = len(files)
        log.info(f'{total} total files found...')
        log.info('Attempting to copy...')
        for i, fname in enumerate(files):
            f = conf['DATA_OUTPUT'] + os.path.basename(fname)
            print(f'Downloading {i + 1} of {total}: {fname}...')
            print(f'Saving copy at: {f}')
            self.ftp.retrbinary('RETR ' + fname, open(f, 'wb').write)
        # quit ftp and return
        self.ftp.quit()
        return

# ftp = Ftp()
# ftp.connect()
# ftp.fetch_files()
