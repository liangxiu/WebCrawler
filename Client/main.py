import Client.china_brand.crawl_pingpai_detail as crawler
#import Client.china_brand.crawl_brand as crawler
import sys

if __name__ == '__main__':
    client_name = sys.argv[1]
    crawler.run(client_name)
