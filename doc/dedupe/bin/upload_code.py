#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ssh import SSH

ip = '222.30.48.9'
usr = 'mjw'
port = 9030
pwd = 'missing'

src_dir='/home/mjwtom/PycharmProjects/swift/'
dst_dir = '/home/mjw/swift/'

client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
client.connect()
client.transport(src_dir, dst_dir, 'put', True)


usr = 'm'
port = 9150
pwd = 'softraid'

dst_dir = '/home/m/mjwtom/swift/'

client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
client.connect()
client.transport(src_dir, dst_dir, 'put', True)