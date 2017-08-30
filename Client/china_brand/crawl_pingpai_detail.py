from Client.crawl_base import Client
from bs4 import BeautifulSoup

BASE_URL = 'https://www.chinapp.com'


def form_url(src, page):
    if page == 1:
        return BASE_URL + src
    else:
        return None


def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    div = soup.find('div', class_='dq_tencot')
    if div is None:
        return None
    lis = div.find_all('li')
    data = []
    for li in lis:
        p = li.find('p')
        if p is None:
            continue
        content = p.string
        if content is None:
            continue
        data.append({'brand_desc': content.strip()})
    return data


def run(client_name):
    client = Client(src_table='pingpai_class', src_primary='href_suffix', dest_table='pingpai_detail', client_name=client_name)
    client.crawl(form_url_func=form_url, parse_html_func=parse_html)

