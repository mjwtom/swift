#!/usr/bin/python
'''
Created on Jan 12, 2015

@author: mjwtom
'''
import os
import pickle

pickle_path = '/home/mjwtom/result/result/upload_result.pickle'

fin = open(pickle_path)
result = pickle.load(fin)
fin.close()
files = [f['file'][1:] for f in result]
for f in files:
    cmd = 'swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing download mjw %s' % f
    os.system(cmd)
