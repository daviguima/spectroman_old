# 3rd party libraries
import numpy as np

# internal modules
from data import get_wl_dat, wl_dat_5
from util import gen_columns, gen_calib_cols

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
    'Batt  V': ['Batt'],
    'Temp °C': ['Temp_Box'],
    'Piranômetro W/m²': ['Tens_Pira'],
    'Ed 450': ['ed_450'],
    'Ed 650': ['ed_650'],
    'Ed 850': ['ed_850'],
    'Ld 450': ['ld1_450', 'ld2_450'],
    'Ld 650': ['ld1_650', 'ld2_650'],
    'Ld 850': ['ld1_850', 'ld2_850'],
    'Lu 450': ['lu1_450', 'lu2_450'],
    'Lu 650': ['lu1_650', 'lu2_650'],
    'Lu 850': ['lu1_850', 'lu2_850'],
    'Rss 450': ['rss1_450', 'rss2_450'],
    'Rss 650': ['rss1_650', 'rss2_650'],
    'Rss 850': ['rss1_850', 'rss2_850'],
    'Sss': ['css1', 'css2']}

monthly_graph_dict = {'keys': ['css1', 'css2'],
                      'ylim': [0, 500]}

def get_intp_selection():
    colls = [c for c in intp_columns]
    dict = {'_id': 0}
    for c in colls:
        dict[c] = 1
    return dict

def get_daily_selection():
    cols = [c for c in daily_columns]
    dict = {'_id': 0, 'TIMESTAMP':1}
    for c in cols:
        dict[c] = 1
    return dict

def get_css_selection():
    cols = [c for c in daily_graph_dict['Sss']]
    dict = {'_id': 0, 'TIMESTAMP':1}
    for c in cols:
        dict[c] = 1
    return dict

def noteq_filter():
    # query generation
    colls = [c for c in calib_cols]
    filter = [{c: {"$ne": np.nan}} for c in colls]
    return { "$and": filter}
