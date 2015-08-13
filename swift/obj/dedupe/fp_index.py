__author__ = 'mjwtom'

import sqlite3
import unittest


class fp_index:
    def __init__(self, name):
        if name.endswith('.db'):
            self.name = name
        else:
            self.name = name + '.db'
        self.conn = sqlite3.connect(name)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS fp_index (key text, value text)''')


    def insert(self, key, value):
        data = (key, value)
        self.c.execute('INSERT INTO fp_index VALUES (?, ?)', data)
        self.conn.commit()

    def lookup(self, key):
        data = (key,)
        self.c.execute('SELECT value FROM fp_index WHERE key=?', data)
        return self.c.fetchone()


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
