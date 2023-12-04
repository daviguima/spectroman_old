import numpy as np
import pandas as pd

def csv_to_df(csv):
    """
    Read csv file and parse it to pandas data frame.
    """
    with open(csv, encoding="utf-8") as f:
        df = pd.read_csv(f,
                         sep=',',
                         skiprows=1,
                         on_bad_lines='error',
                         na_values=[-99, '',
                                    '-INF', 'INF',
                                    '-inf', 'inf'
                                    'Smp', 'TS'],
                         keep_default_na=True)
    return df

def clean_df(df):
    """
    Remove NA values from the data frame (df).
    """
    df.dropna(axis=0, how='any', inplace=True)
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
