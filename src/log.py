import os
import logging

from datetime import datetime
from conf import conf

class Log():
    def __init__(self, fname):
        self.fname = fname
        self.log = logging.getLogger(fname)
        self.fmt = logging.Formatter('%(asctime)s - %(message)s',
                                     datefmt='%H:%M:%S')

    def set_file_log_handler(self):
        file_handler = logging.FileHandler(self.fname)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(self.fmt)
        self.log.addHandler(file_handler)
        return

    def set_console_log_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(self.fmt)
        self.log.addHandler(console_handler)
        return

    def get_log(self):
        self.log.setLevel(logging.INFO)
        return self.log

def log_file():
    time_tag = datetime.now().strftime('%Y%m%dT%H%M%S')
    if os.path.exists(conf['LOG_OUTPUT']):
        log_file = conf['LOG_OUTPUT']
    else:
        print(f'Warning: LOG_OUTPUT path not found inside settings.ini!')
        log_file = os.getcwd()
        # return the path
    return os.path.join(log_file, 'spectroman_' + time_tag + '.log')

def log_init():
    log = Log(log_file())
    log.set_file_log_handler()
    log.set_console_log_handler()
    return log.get_log()

log = log_init()
