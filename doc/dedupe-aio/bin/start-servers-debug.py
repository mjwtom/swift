#!/usr/bin/python
import sys
sys.path.append('../..')
from dedupe.bin.ssh import SSH

from threading import Thread

ip = '127.0.0.1'
port = 22
usr='mjwtom'
password = 'missing1988'


def run_server(cmd=None, conf=None):
    client = SSH(usr=usr, ip=ip, pwd=password, port=port)
    cmd = cmd + ' ' + conf
    stdin, stdout, stderr = client.execute(cmd)
    for l in stdout:
        print 'stdout: %s' % l.strip()
        if 'Total' in l:
            stdin.write('Y\n')
            stdin.flush()
    for l in stderr:
        print 'stderr: %s' % l.strip()


def start_all():
    object_servers = []
    container_servers = []
    account_servers = []
    proxy_servers = []
    servers = ['object-server', 'container-server', 'account-server']
    for x in range(1, 5):
        for server in servers:
            print 'starting %s %d' % (server, x)
            cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-%s' % server
            conf = '/home/mjwtom/PycharmProjects/swift/doc/dedupe-aio/swift/%s/%d.conf' % (server, x)
            args = (cmd, conf)
            if server == 'object-server':
                object_servers.append(Thread(target=run_server, args=args))
            elif server == 'container-server':
                container_servers.append(Thread(target=run_server, args=args))
            elif server == 'account-server':
                account_servers.append(Thread(target=run_server, args=args))
    cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-proxy-server'
    conf = '/home/mjwtom/PycharmProjects/swift/doc/dedupe-aio/swift/proxy-server.conf'
    args = (cmd, conf)
    proxy_servers.append(Thread(target=run_server, args=args))
    threads = []
    threads.extend(object_servers)
    threads.extend(container_servers)
    threads.extend(account_servers)
    threads.extend(proxy_servers)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

start_all()
