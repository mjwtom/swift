#!/usr/bin/python
import sys
sys.path.append('../..')
from test.dedupe.ssh import SSH

from threading import Thread

ip = '127.0.0.1'
port = 22
usr='mjwtom'
password = 'missing1988'


def run_cmd(cmd=None):
    client = SSH(usr=usr, ip=ip, pwd=password, port=port)
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
            cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-%s ' \
                  '/home/mjwtom/PycharmProjects/swift/test/dedupe-aio/swift/%s/%d.conf' \
                  % (server, server, x)
            args = (cmd,)
            if server == 'object-server':
                object_servers.append(Thread(target=run_cmd, args=args))
            elif server == 'container-server':
                container_servers.append(Thread(target=run_cmd, args=args))
            elif server == 'account-server':
                account_servers.append(Thread(target=run_cmd, args=args))
    cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-proxy-server'
    conf = '/home/mjwtom/PycharmProjects/swift/test/dedupe-aio/swift/proxy-server.conf'
    args = (cmd, conf)
    # proxy_servers.append(Thread(target=run_cmd, args=args))
    threads = []
    threads.extend(object_servers)
    threads.extend(container_servers)
    threads.extend(account_servers)
    threads.extend(proxy_servers)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def stop_all():
    threads = []
    servers = ['object-server', 'container-server', 'account-server', 'proxy-server']
    for server in servers:
        print 'killing the servers: %s' % server
        cmd = 'ps -aux | grep swift-%s | grep -v grep | cut -c 9-15 | xargs kill -s 9' % server
        args = (cmd,)
        threads.append(Thread(target=run_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        exit()
    run_type = sys.argv[1]
    if run_type == 'start':
        start_all()
    elif run_type == 'stop':
        stop_all()
