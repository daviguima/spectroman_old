# 3rd party libraries
import numpy as np

# internal modules
from data import get_wl_dat, wl_dat_5

def gen_columns(param, beg, end):
    """
    Generate columns list.
    """
    return [param + str(i) for i in range(beg, end)]

def gen_calib_cols(param):
    """
    Generate CalibData columns list.
    """
    return ['CalibData_' + param + '(' + str(i) + ')' for i in range(1, 167)]

# parameters columns
ed_cols   = gen_columns('ed_',   410, 941)
ld1_cols  = gen_columns('ld1_',  410, 941)
ld2_cols  = gen_columns('ld2_',  410, 941)
lu1_cols  = gen_columns('lu1_',  410, 941)
lu2_cols  = gen_columns('lu2_',  410, 941)
rss1_cols = gen_columns('rss1_', 410, 941)
rss2_cols = gen_columns('rss2_', 410, 941)

c1_cols = gen_calib_cols('c1')
c2_cols = gen_calib_cols('c2')
c3_cols = gen_calib_cols('c3')
c4_cols = gen_calib_cols('c4')
c5_cols = gen_calib_cols('c5')

calib_cols =\
    ['TIMESTAMP'] + \
    c1_cols + \
    c2_cols + \
    c3_cols + \
    c4_cols + \
    c5_cols

calib_columns =\
    c1_cols + \
    c2_cols + \
    c3_cols + \
    c4_cols + \
    c5_cols

# interpolation set values and parameters table
intp_arr = np.arange(410,941)
intp_set = np.array(intp_arr, dtype='str')
intp_table = \
    [[c1_cols, ed_cols, get_wl_dat('wl8711_01600011.dat')],
     [c2_cols, ld1_cols, get_wl_dat('wl8712_0160022.dat')],
     [c3_cols, ld2_cols, get_wl_dat('wl8713_06900023.dat')],
     [c4_cols, lu1_cols, get_wl_dat('wl8714_06900024.dat')],
     [c5_cols, lu2_cols, wl_dat_5()]]

# rss parameters table (or rss input/output columns)
rss_param_table =\
    [[ed_cols + ld1_cols + lu1_cols, rss1_cols],
     [ed_cols + ld2_cols + lu2_cols, rss2_cols]]

base_graph_table = [['ed',   'ED - ',   ed_cols],
                    ['ld1',  'LD1 - ',  ld1_cols],
                    ['ld2',  'LD2 - ',  ld2_cols],
                    ['lu1',  'LU1 - ',  lu1_cols],
                    ['lu2',  'LU2 - ',  lu2_cols],
                    ['rss1', 'RSS1 - ', rss1_cols],
                    ['rss2', 'RSS2 - ', rss2_cols]]

intp_columns =\
    ed_cols   + \
    ld1_cols  + \
    ld2_cols  + \
    lu1_cols  + \
    lu2_cols  + \
    rss1_cols + \
    rss2_cols

daily_columns = ['Batt',
                 'Temp_Box',
                 'Tens_Pira',
                 'ed_450',
                 'ed_650',
                 'ed_850',
                 'ld1_450',
                 'ld1_650',
                 'ld1_850',
                 'ld2_450',
                 'ld2_650',
                 'ld2_850',
                 'lu1_450',
                 'lu1_650',
                 'lu1_850',
                 'lu2_450',
                 'lu2_650',
                 'lu2_850',
                 'rss1_450',
                 'rss1_650',
                 'rss1_850',
                 'rss2_450',
                 'rss2_650',
                 'rss2_850',
                 'css1',
                 'css2']

daily_graph_dict = {
    'Batt  V': {'cols': ['Batt'],
                'xlim': [None, None],
                'ylim': [None, None]},
    'Temp °C': {'cols': ['Temp_Box'],
                'xlim': [None, None],
                'ylim': [None, None]},
    'Piranômetro W/m²': {'cols': ['Tens_Pira'],
                         'xlim': [None, None],
                         'ylim': [None, None]},
    'Ed 450': {'cols': ['ed_450'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Ed 650': {'cols': ['ed_650'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Ed 850': {'cols': ['ed_850'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Ld 450': {'cols': ['ld1_450', 'ld2_450'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Ld 650': {'cols': ['ld1_650', 'ld2_650'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Ld 850': {'cols': ['ld1_850', 'ld2_850'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Lu 450': {'cols': ['lu1_450', 'lu2_450'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Lu 650': {'cols': ['lu1_650', 'lu2_650'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Lu 850': {'cols': ['lu1_850', 'lu2_850'],
               'xlim': [None, None],
               'ylim': [None, None]},
    'Rss 450': {'cols': ['rss1_450', 'rss2_450'],
                'xlim': [None, None],
                'ylim': [0.0, 0.05]},
    'Rss 650': {'cols': ['rss1_650', 'rss2_650'],
                'xlim': [None, None],
                'ylim': [0.0, 0.05]},
    'Rss 850': {'cols': ['rss1_850', 'rss2_850'],
                'xlim': [None, None],
                'ylim': [0.0, 0.05]},
    'Sss': {'cols': ['css1', 'css2'],
            'xlim': [None, None],
            'ylim': [0.0, 500.0]}}

monthly_graph_dict = {'keys': ['css1', 'css2'],
                      'ylim': [0, 500]}
