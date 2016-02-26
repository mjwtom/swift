#!/usr/bin/python
from subprocess import Popen, PIPE, call

import os
import sys


if __name__ == '__main__':
    account_server_num = 4
    container_server_num = 4
    object_server_num = 4
    sys.path.append('/home/mjwtom/PycharmProjects/swift/')
    print ('Reseting Swift......')
    print ('Done')
    print ('Starting Proxy Server......')
    cmd = ['/home/mjwtom/PycharmProjects/swift/bin/swift-proxy-server',
          '/home/mjwtom/PycharmProjects/swift/doc/dedupe/swift/proxy-server.conf']
    proxy=Popen(cmd)
    print ('Done')
    accounts = []
    for i in range(account_server_num):
        print ('start account server %d' % i)
        cmd = ['/home/mjwtom/PycharmProjects/swift/bin/swift-account-server',
              '/home/mjwtom/PycharmProjects/swift/doc/dedupe/swift/account-server/%d.conf' % i]
        r = Popen(cmd)
        accounts.append(r)
    print ('Done')
    containers = []
    for i in range(container_server_num):
        cmd = ['/home/mjwtom/PycharmProjects/swift/bin/swift-container-server',
              '/home/mjwtom/PycharmProjects/swift/doc/dedupe/swift/container-server/%d.conf' % i]
        r = Popen(cmd)
        containers.append(r)
    print ('Done')
    objects = []
    for i in range(object_server_num):
        cmd = ['/home/mjwtom/PycharmProjects/swift/bin/swift-object-server',
              '/home/mjwtom/PycharmProjects/swift/doc/dedupe/swift/object-server/%d.conf' % i]
        r = Popen(cmd)
        objects.append(cmd)
    print ('Done')


    # wait for the process to end