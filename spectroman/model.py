import math
import numpy as np

from spectroman.conf import *

def calc_reflectance(ed, ld, lu, rho=0.028):
    """
    Compute the reflectance.
    """
    try:
        rrs = (lu - rho * ld) / ed
    except Exception as e:
        rrs = np.nan
        return rrs
    return rrs

def calc_rrs_reflectance(lst):
    """
    Wrapper function to be used by pandas.DataFrame.apply.
    Given a list of values [ed, ld, lu], compute rss
    reflectance.
    """
    arr = np.array(np.array_split(np.array(lst), 3))
    return np.apply_along_axis(
        lambda x: calc_reflectance(x[0], x[1], x[2]), 0, arr).round(3)

def css_jirau(s):
    "Compute css_jirau using a list of values [rrs850, rrs650]."
    try:
        rrs_ir_850 = s['Rrs850']
        rrs_red_650 = s['Rrs650']
        css = 13.294 * math.exp(5.2532 * (rrs_ir_850 / rrs_red_650))
        css = round(css, 2)  # trimm precision to 2 decimals
    except Exception as e:
        print(e)
        css = np.nan
        if css < 0:
            css = np.nan
        if css > 2000:
            css = 2001.99
    return css

def chla_gitelson(rs665, rs715, rs750):
    # Gitelson 2008
    # RED, RED_EDGE_1, RED_EDGE_2
    chl = 23.1 + 117.4 * (1 / rs665 - 1 / rs715) * rs750
    chl = np.where(chl < 0, np.nan, chl)
    return chl

def nechad(rs700):
    # Nechad 2010
    # water leaving reflectance = np.pi * Rrs
    # Lu / Ed
    ssd = ((445.11 * (np.pi * rs700)) / (1 - ((np.pi * rs700) / 0.1864))) + 1.13
    return ssd

def castillo(rs510, rs670):
    # Castillo 2008
    # http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.1061.814&rep=rep1&type=pdf
    # On the use of ocean color remote sensing to measure the transport of
    # dissolved organic carbon by the Mississippi River Plume
    cdom412 = -0.9 * (rs510 / rs670) + 2.34
    return cdom412

def find_nearest(array, value):
    """
    Small trick to find the closest column to the correct one.
    """
    array = np.asarray(array)
    index = (np.abs(array - value)).argmin()
    return index
