import tornado.ioloop
import tornado.web
import json
import os
import common_db_oper
import codecs


class BaseHandler(tornado.web.RequestHandler):
    def prepare(self):
        type_str = "Content-Type"
        if type_str in self.request.headers and \
                self.request.headers[type_str].startswith("application/json"):
            self.json_args = json.loads(self.request.body)
        else:
            self.json_args = None


# class DistrictHandler(BaseHandler):
#     def get(self):
#         client_name = self.get_query_argument('client_name')
#         dis_id = db_oper.get_one_idle_district()
#         db_oper.create_crawl_state_record(dis_id, client_name)
#         print("client '%s' ask for a district %d" % (client_name, dis_id))
#         self.write(str(dis_id))
#
#     def post(self):
#         dis_id = self.json_args["dis_id"]
#         records = self.json_args['data']
#         if db_oper.insert_poi_many(dis_id, records):
#             db_oper.set_district_craw_done(dis_id)
#             # print('Done for crawling with dis_id %d with items: %d' % (dis_id, len(records)))
#         else:
#             print('Error!!! insert to server failed for dis_id: %d' % dis_id)
#
#
# class StateHandler(BaseHandler):
#     def post(self):
#         current = self.json_args['current']
#         total = self.json_args['total']
#         dis_id = self.json_args['district_id']
#         db_oper.update_crawl_state(dis_id, current, total)
#         # print("state '%dth/%d' for district id: %d" % (current, total, dis_id))
#
#
# class POIHandler(BaseHandler):
#     def get(self, *args, **kwargs):
#         client_name = self.get_query_argument('client_name')
#         poi_id = db_oper.get_one_poi_id()
#         db_oper.update_poi_craw_state(client_name, 1)
#         # print("POI Crawler %s get poi_id %d" % (client_name, poi_id))
#         self.write(str(poi_id))
#
#     def post(self, *args, **kwargs):
#         poi_id = self.json_args['poi_id']
#         loc_earth = self.json_args['loc_earth']
#         loc_mars = self.json_args['loc_mars']
#         loc_baidu = self.json_args['loc_baidu']
#         # print("poi_id %d done" % poi_id)
#         db_oper.insert_poi_loc(poi_id, loc_earth, loc_mars, loc_baidu)
#
#
# class POISHandler(BaseHandler):
#     def post(self, *args, **kwargs):
#         locs = self.json_args['data']
#         db_oper.insert_locs(locs)
#         print("got locs size: %d" % len(locs))
#
#     def get(self, *args, **kwargs):
#         client = self.get_query_argument('client_name')
#         poi_ids = db_oper.find_mul_poi_ids()
#         if len(poi_ids) > 10:
#             db_oper.update_poi_craw_state(client, 50)
#         print("%s get poi_ids to crawl" % client)
#         self.write(poi_ids)
#
#
# class ExecptionHandler(BaseHandler):
#     def post(self, *args, **kwargs):
#         client = self.json_args['client_name']
#         if 'trace' in self.json_args:
#             trace = self.json_args['trace']
#         elif 'no_list_group_html' in self.json_args:
#             trace = self.json_args['no_list_group_html']
#         else:
#             trace = self.json_args['except_html']
#         file_name = "errors/" + client + ".txt"
#         f = open(file_name, 'a')
#         f.write(trace.encode('utf-8'))
#         f.flush()
#         f.close()
#         print("client %s failed" % client)


class ClientControllerHandler(BaseHandler):
    def post(self, *args, **kwargs):
        client = self.json_args['client_name']
        state = self.json_args['state']
        common_db_oper.update_client_state(client, state)

    def get(self, *args, **kwargs):
        client = self.get_query_argument('client_name')
        cmd = common_db_oper.fetch_client_cmd(client)
        self.write(cmd)


class DataInOutHandler(BaseHandler):
    @staticmethod
    def process_data(info, is_update):
        table = info['table']
        primary_key = None
        if 'primary_key' in info:
            primary_key = info['primary_key']
        records = info['records']
        if is_update:
            common_db_oper.update_record_state_done(table, primary_key, records)
        else:
            common_db_oper.store_records(table, primary_key, records)

    def post(self, *args, **kwargs):
        if self.json_args is not None:
            if 'this' in self.json_args:
                self.process_data(self.json_args['this'], is_update=False)
                if 'last' in self.json_args:
                    self.process_data(self.json_args['last'], is_update=True)

    def get(self):
        table = self.get_query_argument('table', default=None)
        primary = self.get_query_argument('primary_key', default=None)
        if table is not None and primary is not None:
            count = self.get_query_argument('count', default=0)
            ask_all_fields = self.get_query_argument('all_fields', default='0') == '1'
            res = common_db_oper.fetch_records(table, primary=(None if ask_all_fields else primary), count=int(count))
            if res is not None:
                common_db_oper.update_record_state_doing(table=table, primary=primary, records=res)
                self.write(json.dumps(res))


class LogHandler(BaseHandler):
    def post(self, *args, **kwargs):
        client = self.json_args['client_name']
        log_str = self.json_args['log']
        dir_path = 'runtime'
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        file_name = dir_path + '/' + client + '.log'
        with codecs.open(file_name, 'a', 'utf-8') as log_f:
            log_f.write(log_str + '\n')
            log_f.flush()





# class NumberTypeHandler(BaseHandler):
# 	def post(self, *args, **kwargs):
# 		client = self.json_args['client_name']
# 		data = self.json_args['data']
# 		header = self.json_args['header']
# 		db_oper.insert_number_types(data, header)
# 		print 'client %s send types: %d' % (client, len(data))
#
# 	def get(self, *args, **kwargs):
# 		client = self.get_query_argument('client_name')
# 		header = db_oper.fetch_number_base()
# 		print 'client %s got number base: %s' % (client, header)
# 		self.write(str(header))


def make_app():
    return tornado.web.Application([
        # (r"/district", DistrictHandler),
        # (r'/state', StateHandler),
        # (r'/pois', POISHandler),
        # (r'/poi', POIHandler),
        # (r'/exception', ExecptionHandler),
        # (r'/number_type', NumberTypeHandler),
        (r'/data', DataInOutHandler),
        (r'/client', ClientControllerHandler),
        (r'/log', LogHandler),
        (r'/static/(.*)', tornado.web.StaticFileHandler,
         {'path': os.path.join(os.path.dirname(__file__), "static/")}),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()
