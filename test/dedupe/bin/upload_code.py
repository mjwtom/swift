#!/usr/bin/env python
# -*- coding: utf-8 -*-

from test.dedupe.ssh import SSH

ip = '222.30.48.9'
usr = 'mjwtom'
port = 9030
pwd = 'missing1988'

src_dir='/home/mjwtom/PycharmProjects/swift/'
dst_dir = '/home/mjwtom/swift/'

client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
client.transport(src_dir, dst_dir, 'put', True)