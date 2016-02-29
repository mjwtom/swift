#!/usr/bin/env python
# -*- coding: utf-8 -*-

from test.dedupe.ssh import uploads

ip = '222.30.48.9'
usr = 'mjwtom'
port = 9030
pwd = 'missing1988'

tasks = [('/home/mjwtom/PycharmProjects/swift/', '/home/mjwtom/swift/'),
         ('/home/mjwtom/PycharmProjects/python-swiftclient/', '/home/mjwtom/python-swiftclient/')]

uploads(usr, ip, port, pwd, tasks)