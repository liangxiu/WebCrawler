from crawl_base import Client
import util
from china_brand import crawl_brand
if __name__ == '__main__':
    # client = Client(dest_primary='test_id', dest_table='test_table')
    # html = client.request_html('http://www.poi86.com/')
    # data = [{'test_id': 1, 'column_1': 'test_content'}]
    # client.post_data(data)


    # pack_data = [i for i in range(0, 2339)]
    #
    # def func(data):
    #     print len(data)
    # util.do_in_sub_range(1000, pack_data, func)


    # client = Client(client_name='test_log', dest_table='test_table', dest_primary='test_id')
    # client.log('test log 2')


    crawl_brand.run()
