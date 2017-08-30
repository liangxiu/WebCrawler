from zipfile import ZipFile
from StringIO import StringIO
import util
import time
import shutil
import os
import sys
import json
import subprocess


def is_windows():
    return os.name.startswith('nt')


def is_mac():
    name = os.name
    return name.lower().startswith('posix')


def execute_os_cmd(_cmd):
    p_ = subprocess.Popen(_cmd, shell=True)
    return p_


def download_and_unzip_file(url_, path):
    try:
        zfobj = ZipFile(StringIO(util.get_html(url_)))
        for name in zfobj.namelist():
            if name.endswith('/'):
                sub_path = os.path.join(path, name)
                if not os.path.exists(sub_path):
                    os.mkdir(sub_path)
                continue
            uncompressed = zfobj.read(name)
            out_ = path + '/' + name
            output = open(out_, 'wb')
            output.write(uncompressed)
            output.close()
        return True
    except Exception, e:
        print e
        return False


def prepare_dir(dir_):
    if not os.path.exists(dir_):
        os.makedirs(dir_)


def terminate_process():
    if is_process_alive():
        if is_windows():
            subprocess.Popen("TASKKILL /F /PID {pid} /T".format(pid=p.pid))
        else:
            p.terminate()


def remove_dir(dir_):
    if os.path.exists(dir_):
        try:
            shutil.rmtree(dir_)
        except Exception, e:
            print e


def start_run_process():
    return execute_os_cmd(sys.executable + (' %s/Client/main.py %s' % (dir_path, client_name)))


def is_process_alive():
    if p is None:
        return False
    else:
        p.poll()
        if p.returncode is not None:
            return False
        else:
            return True


if __name__ == '__main__':
    dir_path = 'download_space'
    last_cmd = None
    client_name = sys.argv[1]
    HTTP_SERVER = 'http://localhost:8080'
    p = None

    while True:
        post_state = 'beeping'
        cmd = util.get_html(HTTP_SERVER + '/' + 'client?client_name=%s' % client_name)
        if cmd.startswith('dr:'):
            terminate_process()
            p = None
            para = cmd.replace('dr:', '')
            prepare_dir(dir_path)
            if download_and_unzip_file(HTTP_SERVER + '/static/' + para, dir_path):
                p = start_run_process()
                post_state = cmd
            else:
                remove_dir(dir_path)
        elif cmd.startswith('t:'):
            terminate_process()
            p = None
            post_state = cmd
        elif cmd.startswith('c:'):
            terminate_process()
            p = None
            remove_dir(dir_path)
            post_state = cmd
        elif cmd.startswith('restart:'):
            if is_process_alive():
                terminate_process()
                p = None
            if os.path.exists(dir_path):
                p = start_run_process()
                post_state = 'restart'
        elif cmd == '' and not is_process_alive():
            post_state = 'stopped'
            if p is None and last_cmd != 't:':
                #Troy has just started
                if os.path.exists(dir_path):
                    p = start_run_process()
                    post_state = 'auto_restart'

        if cmd != '':
            last_cmd = cmd
        print 'beeping' if post_state == 'stopped' else post_state
        if post_state == 'beeping' and is_process_alive():
            post_state = 'running'
        util.post_json(HTTP_SERVER + '/client', json.dumps({'client_name': client_name, 'state': post_state}))
        time.sleep(10)

    print "main process done"


