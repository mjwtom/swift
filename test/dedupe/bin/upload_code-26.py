#!/usr/bin/env python
# -*- coding: utf-8 -*-

from test.dedupe.ssh import uploads
import os

ip = '222.30.48.9'
usr = 'mjw'
port = 9026
pwd = 'missing'

src = '/home/mjwtom/PycharmProjects/swift/'
dst = '/home/mjw/swift/'

tasks = []

for file in os.listdir(src):
    if file == '.git':
        continue
    src_sub = os.path.join(src, file)
    dst_sub = os.path.join(dst, file)
    tasks.append((src_sub, dst_sub))

#tasks = [('/home/mjwtom/PycharmProjects/swift/', '/home/mjwtom/swift/')]


uploads(usr, ip, port, pwd, tasks)
