import os

clients = ["QiangYe"]


def generate_bat_file(client_):
    str_ = "@echo off\n" \
           "C:\Python27\python.exe main.py %s\n" \
           "pause" % client_

    f = open('troy.bat', 'w')
    f.write(str_)
    f.flush()
    f.close()


def generate_bats(client_):
    generate_bat_file(client_)


def zip_files(client_):
    os.system("zip packages/troy_%s.zip "
              "main.py "
              "util.py troy.bat"
              % client_)

if __name__ == "__main__":
    #os.system("rm -rf POI_Crawler/html POI_Crawler/poi")
    for client_name in clients:
        generate_bats(client_name)
        zip_files(client_name)

