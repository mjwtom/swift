#!/usr/bin/python
'''
Created on Jan 12, 2015

@author: mjwtom
'''
import os

if __name__ == '__main__':
    os.system('swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing upload mjw ${HOME}/file')
