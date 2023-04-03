import math
import numpy as np


class Equations:
    def __init__(self):
        pass

    @staticmethod
    def calc_reflectance(ed, lu, ld, rho=0.028):
        rrs = (lu - rho * ld) / ed
        return rrs

    @staticmethod
    def css_jirau(rs850, rs650):
        css = 13.294 * math.exp(5.2532 * (rs850 / rs650))
        return css

    @staticmethod
    def chla_gitelson(rs665, rs715, rs750):
        # Gitelson 2008
        # RED, RED_EDGE_1, RED_EDGE_2
        chl = 23.1 + 117.4 * (1 / rs665 - 1 / rs715) * rs750
        chl = np.where(chl < 0, np.nan, chl)
        return chl

    @staticmethod
    def nechad(rs700):
        # Nechad 2010
        # water leaving reflectance = np.pi * Rrs
        # Lu / Ed
        ssd = ((445.11 * (np.pi * rs700)) / (1 - ((np.pi * rs700) / 0.1864))) + 1.13
        return ssd

    @staticmethod
    def castillo(rs510, rs670):
        # Castillo 2008
        # http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.1061.814&rep=rep1&type=pdf
        # On the use of ocean color remote sensing to measure the transport of
        # dissolved organic carbon by the Mississippi River Plume
        cdom412 = -0.9 * (rs510 / rs670) + 2.34
        return cdom412

    @staticmethod
    def find_nearest(array, value):
        # small trick to find the closest column to the correct one
        array = np.asarray(array)
        idx = (np.abs(array - value)).argmin()
        return idx
