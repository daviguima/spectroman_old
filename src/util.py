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
from const import *

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
    """
    Move csvs files to another path.
    """
    dir = abspath(curdir)
    for f in files:
        rename(f, join(dir, "files", basename(f)))

def file_to_str_io(fname):
    """
    Return StringIO object created by the fname.
    """
    with open(fname) as f:
        return StringIO(f.read())

def list_date():
    """
    Return a list of csv file name dates.
    """
    dates = []
    for f in list_csvs():
        dates.append(basename(f).split('_')[0])
    return dates

def day2datetime(hour, day, month, year):
    """
    Parse day to datetime regarding the month and year.
    """
    return datetime.strptime(str(day) +
                             '/' + str(month) +
                             '/' + str(year) +
                             ' ' + hour,
                             '%d/%m/%Y %H:%M:%S')

def month_date_pair(year, month):
    """
    Get the date time pear from the first and last day of the
    month.
    """
    cal = calendar.Calendar(6)
    days = [d for d in cal.itermonthdays(year, month) if d != 0]
    return [day2datetime('09:00:00', days[0], month, year),
            day2datetime('16:00:00', days[-1], month, year)]

def get_intp_selection():
    """
    Get interpolation filter columns (mongodb related).
    """
    colls = [c for c in intp_columns]
    dict = {'_id': 0}
    for c in colls:
        dict[c] = 1
    return dict

def get_daily_selection():
    """
    Get columns selection for the daily graph.
    """
    cols = [c for c in daily_columns]
    dict = {'_id': 0, 'TIMESTAMP':1}
    for c in cols:
        dict[c] = 1
    return dict

def get_css_selection():
    """
    Get css selection for the
    """
    cols = [c for c in daily_graph_dict['Sss']]
    dict = {'_id': 0, 'TIMESTAMP':1}
    for c in cols:
        dict[c] = 1
    return dict

def noteq_filter(columns):
    """
    Create a not equal filter using the right columns names.
    """
    cols = [c for c in columns]
    filter = [{c: {"$ne": np.nan}} for c in cols]
    return { "$and": filter}
