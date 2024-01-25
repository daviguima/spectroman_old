# builtin library
from os import listdir
from os.path import isfile, join, abspath, basename
from io import StringIO

# 3rd party libraries
import numpy as np
from scipy.interpolate import griddata

# internal modules
from spectroman.conf import conf

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

def linear_intp(lst, wl, set):
    return griddata(wl,
                    np.array(lst),
                    set,
                    method='linear')

def gen_columns(param, beg, end):
    return [param + str(i) for i in range(beg, end)]

def gen_calib_cols(param):
    return ['CalibData_' + param + '(' + str(i) + ')' for i in range(1, 167)]
