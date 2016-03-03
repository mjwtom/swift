#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-
'''
Created on Jan 12, 2015

@author: mjwtom
'''
import os

if __name__ == '__main__':
    os.system('/home/mjwtom/bin/swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing download mjw home/mjwtom/file')
