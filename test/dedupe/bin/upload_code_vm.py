#!/usr/bin/env python
# -*- coding: utf-8 -*-

from test.dedupe.ssh import uploads
import os

ip = '221.114.21.30'
usr = 'mjwtom'
port = 22
pwd = 'missing1988'


ip = '120.24.80.98'

tasks = [('/home/mjwtom/PycharmProjects/swift/', '/home/mjwtom/swift/'),
         ('/home/mjwtom/PycharmProjects/python-swiftclient/', '/home/mjwtom/python-swiftclient/'),
         ('/home/mjwtom/Downloads/Python-2.7.11.tgz', '/home/mjwtom/Python-2.7.11.tgz'),
         ('/home/mjwtom/Downloads/setuptools-20.2.1.tar.gz', '/home/mjwtom/setuptools-20.2.1.tar.gz'),
         ('/home/mjwtom/Downloads/pip-8.0.2.tar.gz', '/home/mjwtom/pip-8.0.2.tar.gz')]

tasks = [('/home/mjwtom/PycharmProjects/swift/', '/home/mjwtom/swift/')]

src = '/home/mjwtom/PycharmProjects/swift/'
dst = '/home/mjwtom/swift/'

tasks = []

for file in os.listdir(src):
    if file == '.git':
        continue
    src_sub = os.path.join(src, file)
    dst_sub = os.path.join(dst, file)
    tasks.append((src_sub, dst_sub))

uploads(usr, ip, port, pwd, tasks)