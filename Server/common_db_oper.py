import mysql.connector as connector
import mysql


class Cursor:
    def __enter__(self):
        self.con = connector.connect(host='localhost', user='root', passwd='123456', db='test_scalar_crawler')
        self.cursor = self.con.cursor()
        return self.con, self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.con.close()


def get_sql_type(value):
    sql_type = 'text'
    if type(value) is int:
        sql_type = 'int'
    return sql_type


def check_to_create_table(table, primary_key, records):
    if len(records) > 0:
        item = records[0]
        keys = item.keys()
        create_table = "create table if not exists %s (" % table
        if primary_key is None:
            create_table += 'id int primary key auto_increment,'
        for key in keys:
            sql_type = get_sql_type(item[key])
            if key == primary_key:
                if sql_type == 'text':
                    sql_type = 'varchar(500)'
                create_table += ('%s %s primary key,' % (key, sql_type))
            else:
                create_table += '%s %s,' % (key, sql_type)
        create_table += "state int default 0, create_time timestamp default current_timestamp,"
        create_table += "update_time timestamp default current_timestamp on update current_timestamp"
        create_table += ')'
        with Cursor() as (_, cur):
            cur.execute(create_table)


def filter_text(content):
    if content is None:
        return None
    else:
        string = ""
        for c in content:
            if c == '"':
                string += '\\\"'
            elif c == "'":
                string += "\\\'"
            elif c == "\\":
                string += "\\\\"
            else:
                string += c
        return string


def construct_insert(table, record):
    insert_sql = "insert into %s (" % table
    for key in record.keys():
        insert_sql += "%s," % key
    insert_sql = insert_sql[0:-1]
    insert_sql += ') values('
    for key in record.keys():
        val = record[key]
        if get_sql_type(val) == 'text':
            insert_sql += "'%s'," % filter_text(val)
        else:
            insert_sql += (str(val) + ",")
    insert_sql = insert_sql[0:-1]
    insert_sql += ")"
    return insert_sql


def form_sql_vals(key, records):
    pri_vals = []
    for r in records:
        if type(r) is dict:
            pri_vals.append(r[key])
        else:
            pri_vals.append(r)
    pri_val_str = ""
    for v in pri_vals:
        val_type = get_sql_type(v)
        if val_type == 'text':
            pri_val_str += "'%s'," % filter_text(v)
        else:
            pri_val_str += "%d," % v
    pri_val_str = pri_val_str[0:-1]
    return pri_val_str


def update_record_state(table, primary, state_val, records):
    if len(records) == 0:
        return
    update_sql = "update %s set state = %d where %s in (%s)" % \
                 (table, state_val, primary, form_sql_vals(primary, records))
    with Cursor() as (con, cur):
        cur.execute(update_sql)
        con.commit()


def store_records(table, primary, records):
    check_to_create_table(table, primary, records)
    with Cursor() as (con, cur):
        for item in records:
            try:
                insert_sql = construct_insert(table, item)
                cur.execute(insert_sql)
            except mysql.connector.Error as e:
                print insert_sql
                print "error: %s" % e

        con.commit()


def fetch_records(table, primary=None, count=0):
    if primary is None:
        select_sql = "select * from %s where state = 0" % table
    else:
        select_sql = "select %s from %s where state = 0" % (primary, table)
    if count > 0:
        select_sql += " limit %d" % count
    with Cursor() as (con, cur):
        cur.execute(select_sql)
        rows = cur.fetchall()
        if rows is not None and len(rows) > 0:
            if primary is not None:
                res = [item[0] for item in rows]
                return res
            res = []
            columns = [t[0] for t in cur.description]
            for item in rows:
                res_item = {}
                for i in range(0, len(item)):
                    if columns[i] in ["state", "update_time", "create_time"]:
                        continue
                    res_item[columns[i]] = item[i]
                res.append(res_item)
            return res


def update_record_state_done(table, primary, records):
    update_record_state(table, primary, 2, records)


def update_record_state_doing(table, primary, records):
    update_record_state(table, primary, 1, records)


def fetch_client_cmd(name):
    with Cursor() as (con, cur):
        create_sql = "create table if not exists client_control(" \
                     "name varchar(200) primary key, " \
                     "cmd varchar(200), " \
                     "last_cmds varchar(500), " \
                     "states varchar(500), " \
                     "create_time timestamp default current_timestamp, " \
                     "update_time timestamp default current_timestamp on update current_timestamp)"
        cur.execute(create_sql)
        sql = "select cmd, last_cmds from client_control where name = '%s'" % name
        cur.execute(sql)
        res = cur.fetchone()
        cmd = ''
        if res is None:
            sql = "insert into client_control (name, cmd) values ('%s', '')" % name
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
        return cmd


def update_client_state(name, state):
    if len(state) == 0:
        return
    with Cursor() as (con, cur):
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


def test_update_record():
    import json
    records_j = '[{"test1":"hello key", "test2":"test2-content"}]'
    records = json.loads(records_j)
    update_record_state_done("test_table_2", "test1", records)


def test_create_table_and_insert():
    import json
    records_j = '[{"test1":"hello key", "test2":"test2-content"}]'
    records = json.loads(records_j)
    check_to_create_table('test_table_2', None, records)
    insert = construct_insert('test_table_2', records[0])
    with Cursor() as (con, cur):
        cur.execute(insert)
        con.commit()


def test_fetch_records():
    import json
    res = fetch_records("test_table_2", count=1, primary='test1')
    print json.dumps(res)


if __name__ == '__main__':
    #test_fetch_records()
    test_create_table_and_insert()



