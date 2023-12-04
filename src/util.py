from os import listdir
from os.path import isfile, join, abspath

from conf import conf

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
