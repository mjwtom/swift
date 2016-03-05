#!/usr/bin/python
import sys
from test.dedupe.ssh import run_cmd
from threading import Thread
import os

ip = '127.0.0.1'
port = 22
usr='mjwtom'
password = 'missing1988'



def start_all():
    servers = dict()
    servers['object-server'] = []
    servers['container-server'] = []
    servers['account-server'] = []
    servers['proxy-server'] = []
    server_names = ['object-server', 'container-server', 'account-server']
    for x in range(1, 5):
        for server in server_names:
            print 'starting %s %d' % (server, x)
            cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-%s ' \
                  '/home/mjwtom/PycharmProjects/swift/test/dedupe-aio/swift/%s/%d.conf' \
                  % (server, server, x)
            args = (usr, ip, port, password, cmd)
            servers[server].append(Thread(target=run_cmd, args=args))
    cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-proxy-server ' \
          '/home/mjwtom/PycharmProjects/swift/test/dedupe-aio/swift/proxy-server.conf'
    args = (usr, ip, port, password, cmd)
    servers['proxy-server'].append(Thread(target=run_cmd, args=args))
    threads = [server for server_name, serverlist in servers.items() for server in serverlist]
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
        args = (usr, ip, port, password, cmd)
        threads.append(Thread(target=run_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def clean_dir():
    cmd = 'rm -rf'
    for i in range(1, 5):
        dir = '/home/mjwtom/swift-data/%d/sdb%d' % (i, i)
        if not os.path.exists(dir):
            os.makedirs(dir)
        dir += '/*'
        cmd = cmd + ' ' + dir
    run_cmd('mjwtom', '127.0.0.1', 22, 'missing1988', cmd);


def start_all_except(no_server, no_id):
    servers = dict()
    servers['object-server'] = []
    servers['container-server'] = []
    servers['account-server'] = []
    servers['proxy-server'] = []
    server_names = ['object-server', 'container-server', 'account-server']
    for x in range(1, 5):
        for server in server_names:
            print 'starting %s %d' % (server, x)
            if (server == no_server) and (x == no_id):
                continue
            cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-%s ' \
                  '/home/mjwtom/PycharmProjects/swift/test/dedupe-aio/swift/%s/%d.conf' \
                  % (server, server, x)
            args = (usr, ip, port, password, cmd)
            servers[server].append(Thread(target=run_cmd, args=args))
    if no_server != 'proxy-server':
        cmd = '/home/mjwtom/PycharmProjects/swift/bin/swift-proxy-server ' \
              '/home/mjwtom/PycharmProjects/swift/test/dedupe-aio/swift/proxy-server.conf'
        args = (usr, ip, port, password, cmd)
        servers['proxy-server'].append(Thread(target=run_cmd, args=args))
    threads = [server for server_name, serverlist in servers.items() for server in serverlist]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

except_sever = 'object-server'
except_num = 1

if __name__ == '__main__':
    if len(sys.argv) < 2:
        exit()
    run_type = sys.argv[1]
    if run_type == 'start':
        start_all_except(except_sever, except_num)
    if run_type == 'reset':
        stop_all()
        clean_dir()
        start_all_except(except_sever, except_num)
    elif run_type == 'stop':
        stop_all()
    elif run_type == 'clean':
        clean_dir()
