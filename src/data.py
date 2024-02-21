import warnings
from datetime import datetime

import numpy as np
import pandas as pd

def get_wl_dat(fname):
    """
    Return np.ndarray with wl_dat data.
    """
    fname = 'wl_dat/' + fname
    return np.genfromtxt(fname,
                         delimiter = '  ',
                         skip_header = 30,
                         skip_footer = 60,
                         dtype = 'float64',
                         invalid_raise = False,
                         usecols = 1)

def wl_dat_5():
    """
    Return wl_dat_5.
    """
    return np.round(301.122 + 3.32329 *
                    np.arange(1,256) +
                    0.000352334 *
                    np.arange(1,256)**2 -
                    1.81627e-06 *
                    np.arange(1,256)**3,2)[29:195]

def np_read_from_csv(f):
    """
    Read the CSV using the numpy.genfromtxt.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        arr = np.genfromtxt(f,
                            delimiter = ',',
                            skip_header = 1,
                            dtype = 'str',
                            invalid_raise=False)
        if (arr.ndim == 1):
            arr = np.array([], dtype = 'str')
        else:
            arr = arr[1:]
            if (len(arr) > 0):
                if (arr[0][0] == '\"TS\"' or
                    arr[0][0] == 'TS' or
                    arr[0][0] == '\"\"' or
                    arr[0][0] == ''):
                    arr = arr[1:]
                    if (len(arr) > 0):
                        if (arr[0][0] == '\"\"' or arr[0][0] == ''):
                            arr = arr[1:]
        return arr

def csv_to_df(csv):
    """
    Read csv file and parse it to pandas data frame.
    """
    return pd.read_csv(csv,
                       sep=',',
                       skiprows=1,
                       skip_blank_lines=True,
                       on_bad_lines='error',
                       # dtype=dtype_dict,
                       # memory_map=True,
                       low_memory=False,
                       engine='c',
                       na_values=[-99, '', ' ',
                                  '-INF', 'INF',
                                  '-inf', 'inf',
                                  'Smp', 'TS', 'RN'],
                       keep_default_na=True)

def dict_to_df(data):
    """
    Parse the dictionary to Data Frame.
    """
    return pd.DataFrame(data=data, columns=data.keys(), index=[0])

def clean_df(df):
    """
    Remove NA values from the data frame (df).
    """
    # df = df.drop_duplicates(keep='first')
    df = df.dropna(axis=0, how='any')
    return df

def convert_dtypes(df):
    """
    Convert objects to numeric values.
    """
    df = df.apply(pd.to_numeric, errors='ignore')
    return clean_df(df)

def convert_datetime(df):
    """
    Convert time stamp string to date time.
    """
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='ignore')
    return df

def process_df(df):
    """
    Process data frame: clean, convert objects to numeric values and
    time stamp to datetime.
    """
    return convert_datetime(convert_dtypes(clean_df(df)))
