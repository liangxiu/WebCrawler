import requests
import sqlite3
import time
import os
import random
import codecs
import util
from datetime import datetime
import traceback


class Client:
    SERVER_ROOT = 'http://localhost:8080'
    SERVER_URL = SERVER_ROOT + '/data'
    LOG_URL = SERVER_ROOT + '/log'

    def __init__(self, client_name, dest_table, dest_primary=None, src_table=None, src_primary=None):
        self.con = sqlite3.connect('cache.db')
        c = self.con.cursor()
        create_sql = '''create table if not exists
        web_caches (url varchar(500) primary key, file_name varchar(200), state int default 0, create_time timestamp)'''
        c.execute(create_sql)
        self.con.commit()
        c.close()
        self.dest_table = dest_table
        self.dest_primary = dest_primary
        self.src_table = src_table
        self.src_primary = src_primary
        self.client_name = client_name
        self.cached_urls = []

    def log(self, string):
        timestamp = int(time.time())
        time_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        new_str = time_str + '\t' + string
        print new_str
        try:
            requests.post(self.LOG_URL, json={'client_name': self.client_name, 'log': new_str}, timeout=15)
        except requests.exceptions.RequestException as e:
            print 'post log failed for reason: %s' % e

    def crawl(self, form_url_func, parse_html_func, total_page_func=None):
        try:
            while True:
                src = None
                if self.src_table is not None:
                    # if no available resources, the thread will sleep to wait
                    srces = self.request_src(count=1)
                    src = srces[0]
                data = []
                page = 1
                total_page = 0
                if total_page_func is not None:
                    total_page = total_page_func(src)
                while True:
                    url = form_url_func(src, page)
                    if url is None:
                        break
                    html = self.request_html(url)
                    if html is None:
                        break
                    res = parse_html_func(html)
                    if res is None:
                        self.log('no result for this url, break for next src')
                        break
                    else:
                        self.log("got result len: %d" % len(res))
                        # #check to see the parse result has the src primary key
                        if self.src_primary is not None:
                            for item in res:
                                if self.src_primary not in item:
                                    item[self.src_primary] = src
                        data.extend(res)
                    page += 1
                    if 0 < total_page < page:
                        break

                self.post_data(data)
                if self.src_table is None:
                    break
        except:
            tb = traceback.format_exc()
            self.log(tb)


    def request_src(self, count=1):
        while True:
            try:
                self.log('requesting src from table: %s' % self.src_table)
                r = requests.get(self.SERVER_URL, params={"table": self.src_table,
                                                          "primary_key": self.src_primary, "count": count},
                                 timeout=10)
                if len(r.text) > 0:
                    return r.json()
                else:
                    self.log('no resource to crawl for table: %s sleep 60 seconds and try again' % self.src_table)
                    time.sleep(60)
            except requests.exceptions.RequestException as e:
                self.log('get crawl resource failed with exception: %s\nsleep for 30 seconds and try again' % e.message)
                time.sleep(30)


    # #data should be a list, every record should corresponding to the dest table's row
    def post_data(self, data):
        if data is None or len(data) == 0:
            return
        while True:
            try:
                self.log('posting to table %s with len: %d' % (self.dest_table, len(data)))

                def post(sub_data):
                    pack_data = {'this': {"table": self.dest_table, "records": sub_data}}
                    if self.dest_primary is not None:
                        pack_data['this']['primary_key'] = self.dest_primary
                    if self.src_primary is not None:
                        src_primary_vals = [item[self.src_primary] for item in sub_data]
                        pack_data['last'] = {"table": self.src_table,
                                             "primary_key": self.src_primary, "records": src_primary_vals}
                    requests.post(self.SERVER_URL, json=pack_data, timeout=15)
                util.do_in_sub_range(1000, data, post)
                for url in self.cached_urls:
                    self.rm_html_file(url)
                self.log('post data done')
                break
            except requests.exceptions.RequestException as e:
                sleep_t = 1 * 30
                self.log("post data for table: %s failed with reason: %s, "
                         "sleep for some seconds and continue post" % (self.dest_table, e))
                time.sleep(sleep_t)

    def store_html_file(self, url, html_txt):
        file_name = int(time.time() * 1000)
        dir = "html"
        if not os.path.exists(dir):
            os.mkdir(dir)
        full_name = dir + "/" + str(file_name)
        with codecs.open(full_name, 'w', encoding='utf-8') as f:
            f.write(html_txt)
            f.flush()
        #store the record
        c = self.con.cursor()
        insert_sql = "insert into web_caches(url, file_name, create_time) values('%s', '%s', CURRENT_TIMESTAMP)" % (url, full_name)
        c.execute(insert_sql)
        c.close()
        self.con.commit()

    def get_html_file_name(self, url):
        c = self.con.cursor()
        select_sql = "select file_name from web_caches where url='%s' and state = 0" % url
        c.execute(select_sql)
        res = c.fetchone()
        file_name = None
        if res is not None:
            file_name = res[0]
        c.close()
        return file_name

    def rm_html_file(self, url):
        file_name = self.get_html_file_name(url)
        if file_name is not None:
            if os.path.exists(file_name):
                os.remove(file_name)
            c = self.con.cursor()
            update_sql = "update web_caches set state = 1 where url='%s'" % url
            c.execute(update_sql)
            self.con.commit()
            c.close()

    def read_html_file(self, url):
        file_name = self.get_html_file_name(url)
        if file_name is not None:
            file_in = codecs.open(file_name, 'r', encoding='utf-8')
            txt = file_in.read()
            file_in.close()
            return txt

    def request_html(self, url):
        if len(url) > 0 and (url.startswith('http://') or url.startswith('https://')):
            cached = self.read_html_file(url)
            if cached is not None:
                return cached
            while True:
                try:
                    self.log('requesting url: %s' % url)
                    r = requests.get(url, timeout=15)
                    if len(r.text) > 0:
                        self.cached_urls.append(url)
                        self.store_html_file(url, r.text)
                        return r.text
                    else:
                        return None
                except requests.exceptions.RequestException as e:
                    sleep_t = random.randint(1, 10) * 30
                    self.log('request url: %s failed for reason: %s, '
                             'sleep some seconds and continue requesting' % (url, e))
                    time.sleep(sleep_t)




