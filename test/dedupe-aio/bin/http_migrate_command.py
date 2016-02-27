#!/usr/bin/python
'''
Created on Jan 12, 2015

@author: mjwtom
'''
import os
import httplib

ip = '127.0.0.1'
port = 6010

client = None

try:
    client = httplib.HTTPConnection(ip, port, timeout=30)
    client.request('MIGRATE', 'mjwtom/mjwtom/1')
    resp = client.getresponse()
    print resp.status
    print resp.reason
    print resp.read()
except Exception, e:
    print e
finally:
    if client:
        client.close()
