import numpy as np
import pandas as pd

def csv_to_df(file):
    with open(file, encoding="utf-8") as f:
        df = pd.read_csv(f,
                         sep=',',
                         skiprows=1,
                         on_bad_lines='skip',
                         keep_default_na=True)
    return df

def clean_df(df):
    df.dropna(inplace=True)
    return df

def convert_df_dtypes(df):
    df = df.apply(pd.to_numeric, errors='ignore')
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(inplace=True)
    return df

def convect_df_datetime(df):
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='ignore')
    return df
