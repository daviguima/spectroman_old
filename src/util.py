# builtin library
import calendar
from os import listdir, rename
from os.path import isfile, join, abspath, basename, curdir
from io import StringIO
from datetime import datetime

# 3rd party libraries
import numpy as np
from scipy.interpolate import griddata

# internal modules
from conf import conf

def list_csvs(path=None):
    """
    Given a path (optional) return a list of
    full path files (csvs).
    """
    path = path or conf['DATA_OUTPUT']
    csvs = []
    for f in listdir(path):
        csv = join(abspath(path), f)
        if isfile(csv):
            csvs.append(csv)
    return csvs

def move_csvs(files):
    dir = abspath(curdir)
    for f in files:
        rename(f, join(dir, "files", basename(f)))

def open_file(fname):
    return open(fname)

def file_to_str_io(fname):
    with open(fname) as f:
        return StringIO(f.read())

def list_date():
    date = []
    for f in list_csvs():
        date.append(basename(f).split('_')[0])
    return date

def gen_columns(param, beg, end):
    return [param + str(i) for i in range(beg, end)]

def gen_calib_cols(param):
    return ['CalibData_' + param + '(' + str(i) + ')' for i in range(1, 167)]

def day2datetime(hour, day, month, year):
    return datetime.strptime(str(day) +
                             '/' + str(month) +
                             '/' + str(year) +
                             ' ' + hour,
                             '%d/%m/%Y %H:%M:%S')

def month_date_pair(year, month):
    cal = calendar.Calendar(6)
    days = [d for d in cal.itermonthdays(year, month) if d != 0]
    return [day2datetime('06:00:00', days[0], month, year),
            day2datetime('18:00:00', days[-1], month, year)]
