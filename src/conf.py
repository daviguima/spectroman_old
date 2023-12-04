from decouple import config

__version__ = '2.0.0'

conf = {
    'FTP_HOST': config('FTP_HOST'),
    'FTP_USER': config('FTP_USER'),
    'FTP_PASS': config('FTP_PASS'),
    'FTP_PATH': config('FTP_PATH'),
    'FTP_TODO': config('FTP_TODO'),
    'FTP_DONE': config('FTP_DONE'),
    'FTP_FAIL': config('FTP_FAIL'),
    'DB_URI': config('DB_URI'),
    'DB_NAME': config('DB_NAME'),
    'DB_COLL_DF': config('DB_COLL_DF'),
    'DB_COLL_RAW': config('DB_COLL_RAW'),
    'DB_COLL_MAIN': config('DB_COLL_MAIN'),
    'DB_ATLAS_URI': config('DB_ATLAS_URI'),
    'DB_ATLAS_NAME': config('DB_ATLAS_NAME'),
    'DB_ATLAS_COLL': config('DB_ATLAS_COLL'),
    'LOG_OUTPUT': config('LOG_OUTPUT'),
    'DATA_OUTPUT': config('DATA_OUTPUT')
}

r_ed = 'CalibData_c1(76)'
r_ld = 'CalibData_c2(76)'
r_lu = 'CalibData_c4(76)'
ir_ed = 'CalibData_c1(136)'
ir_ld = 'CalibData_c2(136)'
ir_lu = 'CalibData_c4(136)'
