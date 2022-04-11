import ftplib
from decouple import config


def main():

    HOST = config('HOST_ADDRESS')
    USER = config('USER')
    PASS = config('PASS')
    PATH = config('PATH')

    ftp = ftplib.FTP(host=HOST, user=USER, passwd=PASS)
    ftp.login()
    ftp.cwd(PATH)

    data = []
    ftp.dir(data.append)
    ftp.quit()

    for line in data:
        print(line)


if __name__ == "__main__":
    main()
