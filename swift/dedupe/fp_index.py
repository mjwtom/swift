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
        self.c.execute('''CREATE TABLE IF NOT EXISTS fp_index (key text, value text, obj_hash text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS obj_fps (obj_hash text, fps)''')


    def insert_fp_index(self, key, value, obj_hash):
        data = (key, value, obj_hash)
        self.c.execute('INSERT INTO fp_index VALUES (?, ?, ?)', data)
        self.conn.commit()

    def insert_obj_fps(self, obj_hash, fps):
        data = (obj_hash, fps)
        self.c.execute('INSERT INTO obj_fps VALUES (?, ?)', data)
        self.conn.commit()

    def lookup_fp_index(self, key):
        data = (key,)
        self.c.execute('SELECT value FROM fp_index WHERE key=?', data)
        return self.c.fetchone()

    def lookup_obj_fps(self, obj_hash):
        data = (obj_hash,)
        self.c.execute('SELECT value FROM fp_index WHERE key=?', data)
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
