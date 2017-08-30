from Client.crawl_base import Client
from bs4 import BeautifulSoup


def form_url(src, page):
    if page == 1:
        return 'https://www.chinapp.com/paihang'
    else:
        return None


def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    divs = soup.find_all('div', class_='dq_modlat')
    if divs is None or len(divs) == 0:
        return None
    datas = []
    for div in divs:

        def td_filter(tag):
            return tag.name == 'td' and not tag.has_attr('class')
        tds = div.find_all(td_filter)
        for td in tds:
            a_s = td.find_all('a')
            for a in a_s:
                href = a['href']
                title = a['title']
                if href is not None and title is not None:
                    datas.append({'href_suffix': href, 'brand_title': title.strip()})
    return datas


def run(client_name):
    client = Client(dest_primary='href_suffix', dest_table='pingpai_class', client_name=client_name)
    client.crawl(form_url_func=form_url, parse_html_func=parse_html)
