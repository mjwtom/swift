import sqlite3
from swift.common.utils import config_true_value
from swift.dedupe.disk_hashtable import DiskHashtable


class FingerprintIndex(object):
    def __init__(self, conf):
        self.sqlite_index = config_true_value(conf.get('sqlite_index', False))
        self.db_name = conf.get('data_base', ':memory:')
        if not self.db_name.endswith('.db') and not self.db_name == ':memory:':
            self.db_name += '.db'
        self.conn = sqlite3.connect(self.db_name)
        self.c = self.conn.cursor()
        if self.sqlite_index:
            self.c.execute('''CREATE TABLE IF NOT EXISTS fp_index (fp text PRIMARY KEY NOT NULL, container_id text)''')
            self.fp_buf = dict()
            self.db_max_buf = int(conf.get('db_max_buf_fp', 1024))
        else:
            self.disk_hashtable = DiskHashtable(conf)
        self.c.execute('''CREATE TABLE IF NOT EXISTS obj_fps (obj text PRIMARY KEY NOT NULL, fps text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS obj_etag (obj text PRIMARY KEY NOT NULL, etag text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS dc_rc(dc text PRIMARY KEY NOT NULL, rc text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS dc_device(id int auto_increment primary key not null,
        dc text, dev text)''')

    def insert_fp(self, fp, container_id):
        if self.sqlite_index:
            self.fp_buf[fp] = container_id
            if len(self.fp_buf) >= self.db_max_buf:
                for fp, container_id in self.fp_buf.items():
                    data = (fp, container_id)
                    self.c.execute('INSERT INTO fp_index VALUES (?, ?)', data)
                self.conn.commit()
                self.fp_buf = dict()
        else:
            self.disk_hashtable.add_kv(fp, container_id)

    def insert_obj_fps(self, obj_hash, fps):
        data = (obj_hash, fps)
        self.c.execute('INSERT INTO obj_fps VALUES (?, ?)', data)
        self.conn.commit()

    def lookup_fp(self, fp):
        if self.sqlite_index:
            r = self.fp_buf.get(fp, None)
            if r:
                return r
            data = (fp,)
            self.c.execute('SELECT container_id FROM fp_index WHERE fp=?', data)
            r = self.c.fetchall()
            if r:
                r = r[0][0]
        else:
            r = self.disk_hashtable.lookup(fp)
        return r

    def lookup_obj_fps(self, obj_hash):
        data = (obj_hash,)
        self.c.execute('SELECT value FROM fp_index WHERE obj=?', data)
        r = self.c.fetchall()
        if r:
            r = r[0][0]
        return r

    def insert_etag(self, key, value):
        data = (key, value)
        self.c.execute('INSERT INTO obj_etag VALUES (?, ?)', data)
        self.conn.commit()

    def lookup_etag(self, key):
        data = (key,)
        self.c.execute('SELECT etag FROM obj_etag WHERE obj=?', data)
        r = self.c.fetchall()
        if r:
            r = r[0][0]
        return r

    def insert_rc(self, dc, rc):
        data = (dc, rc)
        self.c.execute('UPDATE dc_rc VALUES(rc=?) WHERE dc=?', data)
        self.conn.commit()

    def get_rc(self, dc):
        data = (dc,)
        self.c.execute('SELECT rc from dc_rc where dc=?', data)
        r = self.c.fetchall()
        if r:
            r = r[0][0]
        return r

    def update_rc(self, dc, rc):
        data = (rc, dc)
        self.c.execute('update dc_rc set rc=? WHERE dc=?', data)
        self.conn.commit()

    def get_all_rc(self):
        self.c.execute('SELECT * FROM dc_container_rc')
        kall = self.c.fetchall()
        return kall

    def get_dev_dc(self, dev):
        data = (dev,)
        self.c.execute('SELECT dc FROM dc_device where dev=?', data)
        kall = self.c.fetchall()
        return kall

    def batch_update_rc(self, dc_rc):
        for (dc, rc) in dc_rc.items():
            rc = str(rc)
            data = (dc,)
            self.c.execute('SELECT dc FROM dc_device where dev=?', data)
            r = self.c.fetchall()
            if r:
                data = (rc, dc)
                self.c.execute('update dc_rc set rc=? where id=?', data)
            else:
                data = (dc, rc)
                self.c.execute('insert into dc_rc values (?, ?)', data)
        self.conn.commit()

    def __del__(self):
        self.conn.close()