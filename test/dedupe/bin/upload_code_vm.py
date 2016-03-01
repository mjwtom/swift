#!/usr/bin/env python
# -*- coding: utf-8 -*-

from test.dedupe.ssh import uploads

ip = '220.113.20.30'
usr = 'mjwtom'
port = 22
pwd = 'missing1988'

tasks = [('/home/mjwtom/PycharmProjects/swift/', '/home/mjwtom/swift/'),
         ('/home/mjwtom/PycharmProjects/python-swiftclient/', '/home/mjwtom/python-swiftclient/'),
         ('/home/mjwtom/Downloads/Python-2.7.11.tgz', '/home/mjwtom/Python-2.7.11.tgz'),
         ('/home/mjwtom/Downloads/setuptools-20.2.1.tar.gz', '/home/mjwtom/setuptools-20.2.1.tar.gz'),
         ('/home/mjwtom/Downloads/pip-8.0.2.tar.gz', '/home/mjwtom/pip-8.0.2.tar.gz')]

uploads(usr, ip, port, pwd, tasks)