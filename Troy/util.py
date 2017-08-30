import urllib2
import time
from random import randint


def get_html(url):
    count = 1
    while True:
        try:
            html = urllib2.urlopen(url, timeout=15).read()
            return html
        except Exception, e:
            error_s = "error %s for url: %s" % (e, url)
            if error_s.find("403") > 0:
                print("sleeping minutes")
                time.sleep(randint(60, 180))
            else:
                if count > 3:
                    return ""
                count += 1
                print("sleeping seconds")
                time.sleep(30)


def post_json(url, data):
    count = 1
    while True:
        try:
            req = urllib2.Request(url)
            req.add_header('Content-Type', 'application/json')
            response = urllib2.urlopen(req, data, timeout=50).read()
            return response
        except Exception, e:
            if count > 3:
                break
            count += 1
            print("error %s for post data to url: %s" % (e, url))
            time.sleep(30)
