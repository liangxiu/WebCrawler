from Client.crawl_base import Client
from bs4 import BeautifulSoup
import re


def form_url(src, page):
    url = '%s/district/%d/%d.html' % ("http://www.poi86.com", src, page)
    return url


def get_total_pages(src):
    url = form_url(src, 1)
    html = client.request_html(url)
    if html is None:
        return 0
    soup = BeautifulSoup(html, 'html.parser')
    ul = soup.find('ul', class_="pagination")
    if ul is None:
        return 0
    lis = ul.contents
    last_li = lis[len(lis)-1]
    a = last_li.contents[0]
    count_str = a.string.strip()
    count = int(re.findall(r'.*/(\d+)', count_str)[0])
    return count


def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', class_='table table-bordered table-hover')
    result = []
    if table is None:
        return result
    else:
        trs = table.find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            if tds is None or len(tds) < 4:
                continue
            a = tds[0].find('a')
            if a is None or a.string is None:
                continue
            title = a.string.strip()
            href = a.attrs.get('href')
            poi_id = int(re.findall(r'/poi/(\d+)\.html', href)[0])
            address = tds[1].string
            if address is None:
                address = ''
            else:
                address = address.strip()

            number = tds[2].string
            if number is not None:
                number = number.strip()
                number = number.replace('(', '')
                number = number.replace(')', '')
            else:
                number = ''

            class_a = tds[3].find('a')
            class_ = ''
            if class_a is not None:
                class_ = class_a.string.strip()
            if len(number) > 30:
                continue
            result.append({'poi_id': poi_id, 'name': title, 'address': address, 'phone': number, 'class': class_})
        return result

if __name__ == '__main__':
    client = Client(src_primary='dis_id', src_table='district', dest_primary='poi_id', dest_table='poi')
    client.crawl(form_url_func=form_url, parse_html_func=parse_html, total_page_func=get_total_pages)
