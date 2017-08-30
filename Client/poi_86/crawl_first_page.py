#coding=utf-8
from Client.crawl_base import Client
from bs4 import BeautifulSoup
import re
import time

POI_URL = 'http://www.poi86.com/'

def parse_citys(html):
    soup = BeautifulSoup(html, 'html.parser')
    lis = soup.find_all("li", class_="list-group-item")
    citys = []
    districts = []
    for item in lis:
        row = item.find('div', class_='row')
        if row is not None:
            head = row.find('div', class_='col-md-2')
            province = head.find('strong').string.strip()
            content = row.find('div', class_='col-md-10')
            for a in content.find_all('a'):
                href = a.attrs.get('href')
                title = a.attrs.get('title').replace(u'POI数据', '').strip()
                if href.find('district') >= 0:
                    dis_id = int(re.findall(r'.*/(\d+)/1\.html', href)[0])
                    #districts.append(DistrictRecord(province, '', title, dis_id))
                    districts.append({'province': province, 'city': province, 'district': title, 'dis_id': dis_id})
                else:
                    dis_id = int(re.findall(r'.*/(\d+)\.html', href)[0])
                    citys.append({'province': province, 'city': title, 'city_id': dis_id})
                    #citys.append(DistrictRecord(province, title, '', dis_id))
    return citys, districts


def parse_districts(city):
    city_url = '%spoi/city/%d.html' % (POI_URL, city['city_id'])
    soup = BeautifulSoup(client.request_html(city_url), 'html.parser')
    lis = soup.find_all('li', class_='list-group-item')
    districts = []
    for item in lis:
        a = item.find('a')
        href = a.attrs.get('href')
        dis_id = int(re.findall(r'.*/(\d+)/1\.html', href)[0])
        name = a.string.strip()
        districts.append({'province': city['province'], 'city': city['city'], 'district': name, 'dis_id': dis_id})
        #districts.append(DistrictRecord('', '', name, dis_id))
    return districts


if __name__ == '__main__':
    client = Client(dest_table='district', dest_primary='dis_id')
    html = client.request_html(POI_URL)
    citys, districts = parse_citys(html)
    print('parsed all citys: %d, part of districts: %d' % (len(citys), len(districts)))
    i = 1
    data = []
    data.extend(districts)
    for item in citys:
        print('getting districts for the %d/%dth city: %s' % (i, len(citys), item['city']))
        districts = parse_districts(item)
        i += 1
        print('got all districts: %d' % len(districts))
        data.extend(districts)
        # records = []
        # for dis in districts:
        #     records.append(DistrictRecord(item.province, item.city, dis.district, dis.district_id))
        # db_oper.insert_many(records)
        print('sleeping 5 seconds')
        time.sleep(1)
    client.post_data(data)

