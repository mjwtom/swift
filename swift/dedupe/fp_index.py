__author__ = 'mjwtom'

import sqlite3
import unittest


class fp_index(object):
    def __init__(self, name):
        if name.endswith('.db') or (name == ':memory:'):
            self.name = name
        else:
            self.name = name + '.db'
        self.conn = sqlite3.connect(self.name)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS fp_index (fp text, container_id text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS obj_fps (obj text, fps text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS obj_etag (obj text, etag text)''')


    def insert_fp_index(self, fp, container_id):
        data = (fp, container_id)
        self.c.execute('INSERT INTO fp_index VALUES (?, ?)', data)
        self.conn.commit()

    def insert_obj_fps(self, obj_hash, fps):
        data = (obj_hash, fps)
        self.c.execute('INSERT INTO obj_fps VALUES (?, ?)', data)
        self.conn.commit()

    def lookup_fp_index(self, fp):
        data = (fp,)
        self.c.execute('SELECT container_id FROM fp_index WHERE fp=?', data)
        return self.c.fetchone()[0]

    def lookup_obj_fps(self, obj_hash):
        data = (obj_hash,)
        self.c.execute('SELECT value FROM fp_index WHERE obj=?', data)
        return self.c.fetchone()[0]

    def insert_etag(self, key, value):
        data = (key, value)
        self.c.execute('INSERT INTO obj_etag VALUES (?, ?)', data)
        self.conn.commit()

    def lookup_etag(self, key):
        data = (key,)
        self.c.execute('SELECT etag FROM obj_etag WHERE obj=?', data)
        return self.c.fetchone()

'''
def testinsert():
    fp = fp_index('/home/mjwtom/mydb.db')
    for i in range(0, 100):
        str = i.__str__()
        fp.insert(str, str)

def testselect():
    fp = fp_index('/home/mjwtom/mydb.db')
    for i in range(0, 100):
        str = i.__str__()
        c = fp.lookup(str)
        for row in c:
            print row



if __name__ == '__main__':
    unittest.main()
'''
