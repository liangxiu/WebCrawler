# coding=utf-8
import mysql.connector


def get_con_cursor():
    con = mysql.connector.connect(host='10.95.217.73', user='pois', passwd='YL5Ic5-j', db='pois')
    cursor = con.cursor()
    return con, cursor


def inner_find_one_idle(sql, cur):
    cur.execute(sql)
    res = cur.fetchone()
    dis_id = 0
    if res:
        dis_id = res[0]
    return dis_id


def get_one_idle_district():
    con, cur = get_con_cursor()
    sql = "select district_id from district where state = 0 and (province = '上海市' or province = '北京市' or city = '广州市' or city = '深圳市') limit 1"
    dis_id = inner_find_one_idle(sql, cur)
    res = None
    if dis_id == 0:
        sql = "select district_id from district where state = 0 and (province = '广东省' or province = '江苏省' or province = '山东省' or province = '浙江省') limit 1"
        dis_id = inner_find_one_idle(sql, cur)
        if dis_id == 0:
            sql = "select district_id from district where state = 0 limit 1"
            dis_id = inner_find_one_idle(sql, cur)
    if dis_id > 0:
        sql = "update district set state = 1 where district_id = %d" % dis_id
        cur.execute(sql)
        con.commit()
    cur.close()
    con.close()
    return dis_id


def insert_poi_many(dis_id, items):
    if len(items) == 0:
        return False
    con, cur = get_con_cursor()
    sql = "insert into poi_item (id, district_id, name, address, phone, class) values (%d, %d, '%s', '%s', '%s', '%s')"
    fail_count = 0
    duplicate = False
    for item in items:
        if len(item['address']) > 50 or len(item['name']) > 50 or len(item['class']) > 50:
            continue
        try:
            address = item['address']
            name = item['name']
            if address.find("'") >= 0:
                address = address.replace("'", "")
            if name.find("'") >= 0:
                name = name.replace("'", "")
            sql_ = sql % (item['id'], dis_id, name, address, item['phone'], item['class'])
            cur.execute(sql_)
        except Exception, e:
            error_s = "error for district %d: %s" % (dis_id, e)
            print(error_s)
            if error_s.find('Duplicate entry') < 0:
                fail_count += 1
            else:
                duplicate = True
                break
    print("inserting poi_items with size:%d duplicate: %d failCount: %d " % (len(items), duplicate, fail_count))
    con.commit()
    cur.close()
    con.close()

    return True


def set_district_craw_done(dis_id):
    con, cur = get_con_cursor()
    sql = "update district set state = 2 where district_id = %d" % dis_id
    cur.execute(sql)
    con.commit()
    cur.close()
    con.close()


def get_district_info(dis_id):
    con, cur = get_con_cursor()
    sql = "select province, city, district from district where district_id = %d" % dis_id
    cur.execute(sql)
    res = cur.fetchone()
    cur.close()
    con.close()
    return res


def create_crawl_state_record(dis_id, client):
    con, cur = get_con_cursor()
    sql = "replace into district_crawl_state (district_id, client_name) values (%d, '%s')" % (dis_id, client)
    cur.execute(sql)
    con.commit()
    cur.close()
    con.close()


def update_crawl_state(dis_id, current, total):
    con, cur = get_con_cursor()
    sql = "update district_crawl_state set " \
          "current_th = %d, total_item = %d where district_id = %d" % \
          (current, total, dis_id)
    cur.execute(sql)
    con.commit()
    cur.close()
    con.close()


def insert_poi_loc(poi_id, loc_earth, loc_mars, loc_baidu):
    con, cur = get_con_cursor()
    try:
        sql = "insert into poi_loc values(%d, '%s', '%s', '%s')" % (poi_id, loc_earth, loc_mars, loc_baidu)
        cur.execute(sql)
        # update poi_item table
        sql = "update poi_item set state = 2 where id = %d" % poi_id
        cur.execute(sql)
        con.commit()
    except Exception, e:
        print ("Error %s for sql: %s" % (e, sql))
        con.rollback()
    cur.close()
    con.close()


def get_one_poi_id():
    con, cur = get_con_cursor()
    sql = "select id from poi_item where state = 0 and phone != '' limit 1"
    cur.execute(sql)
    res = cur.fetchone()
    poi_id = 0
    if res is not None:
        poi_id = res[0]
        sql = "update poi_item set state = 1 where id = %d" % poi_id
        cur.execute(sql)
        con.commit()
    cur.close()
    con.close()
    return poi_id


def update_poi_craw_state(client_name, count=0):
    con, cur = get_con_cursor()
    sql = "select * from poi_crawl_state where client = '%s'" % client_name
    cur.execute(sql)
    res = cur.fetchone()
    if res is not None:
        sql = "update poi_crawl_state set craw_count = craw_count + %d where client = '%s'" % (count, client_name)
        cur.execute(sql)
    else:
        sql = "insert into poi_crawl_state (client) values ('%s')" % client_name
        cur.execute(sql)
    con.commit()
    cur.close()
    con.close()


def insert_locs(locs):
    con, cur = get_con_cursor()
    for loc in locs:
        sql = "replace into poi_loc values(%d, '%s', '%s', '%s')" % (
        loc["poi_id"], loc["loc_earth"], loc["loc_mars"], loc["loc_baidu"])
        sql_update = "update poi_item set state = 2 where id = %d" % loc["poi_id"]
        try:
            cur.execute(sql)
            cur.execute(sql_update)
        except Exception, e:
            print("insert into loc fail: %s" % e)
    con.commit()
    cur.close()
    con.close()


def find_mul_poi_ids():
    con, cur = get_con_cursor()
    sql = "select id from poi_item where state = 0 and phone != '' limit 50"
    cur.execute(sql)
    res = cur.fetchall()
    ids = ""
    if res is not None and len(res) > 0:
        index = 0
        for r in res:
            sql = "update poi_item set state = 1 where id = %d" % r[0]
            cur.execute(sql)
            ids += str(r[0])
            if index < len(res) - 1:
                ids += ','
            index += 1
    con.commit()
    cur.close()
    con.close()
    return ids


def fetch_client_cmd(name):
    con, cur = get_con_cursor()
    sql = "select cmd, last_cmds from client_control where name = '%s'" % name
    cur.execute(sql)
    res = cur.fetchone()
    cmd = ''
    if res is None:
        sql = "insert into client_control (name, cmd) values ('%s', 'dr:type_crawler.zip')" % name
        cur.execute(sql)
        con.commit()
    else:
        cmd = res[0]
        last_cmds = res[1]
        if cmd is not None and len(cmd) > 0:
            if last_cmds is not None:
                update_cmd = cmd + '; ' + last_cmds
            else:
                update_cmd = cmd
	    if len(update_cmd) > 50:
		    update_cmd = update_cmd[:50]
            sql = "update client_control set cmd = '', last_cmds = '%s' where name= '%s'" % (update_cmd, name)
            cur.execute(sql)
            con.commit()
        else:
            cmd = ''
    cur.close()
    con.close()
    return cmd


def update_client_state(name, state):
    if len(state) == 0:
        return
    con, cur = get_con_cursor()
    sql = "select states from client_control where name = '%s'" % name
    cur.execute(sql)
    res = cur.fetchone()
    if res is not None:
        states = res[0]
        if states is None:
            new_states = state
        else:
            new_states = state + '; ' + states
            if len(new_states) > 50:
                new_states = new_states[:50]
        sql = "update client_control set states = '%s' where name = '%s'" % (new_states, name)
        cur.execute(sql)
        con.commit()
    cur.close()
    con.close()

def fetch_number_base():
	con, cur = get_con_cursor()
	sql = "select phone_header from phonenumber_info where state = 0 and city in ('上海', '北京', '深圳', '广州', '杭州') limit 1"
	cur.execute(sql)
	res = cur.fetchone()
	header = 0
	if res is not None:
		header = res[0]
	else:
		sql = "select phone_header from phonenumber_info where state = 0 limit 1"
		cur.execute(sql)
		res = cur.fetchone()
		if res is not None:
			header = res[0]
	if header > 0:
		sql = "update phonenumber_info set state = 1 where phone_header = %d" % header
		cur.execute(sql)
	con.commit()
	cur.close()
	con.close()
	return header

def insert_number_types(recs, header):
	con, cur = get_con_cursor()
	sql = "insert into number_types(num, mark_type, mark_count, comment, source) values ('%s', '%s', %d, '%s', '%s')"
	for rec in recs:
		comment = rec['comment']
		if rec['comment'] > 100:
			comment = rec['comment'][:100]
		insert_sql = sql % (rec['num'], rec['mark_type'], rec['mark_count'], rec['comment'], rec['source'])
		try:
			cur.execute(insert_sql)
		except Exception as e:
			error_s = "error for  number %s: %s" % (rec['num'].decode('utf-8'), str(e).decode('utf-8'))
            		#print(error_s)
            		if error_s.find('Duplicate entry') > 0:		
				break
        con.commit()
	cur.close()
	con.close()


if __name__ == "__main__":
    records = list()
    records.append((12, 'name', 'address', 'phone', 'class1'))
    records.append((13, 'name2', 'address2', 'phone2', 'class2'))
    insert_poi_many(376, records)
    print('done')

