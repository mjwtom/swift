#!/usr/bin/python
import sys
from test.dedupe.ssh import run_cmd
from threading import Thread


ips = ['220.113.20.150',
       '220.113.20.142',
       '220.113.20.144',
       '220.113.20.151',
       '220.113.20.120',
       '220.113.20.121',
       '220.113.20.122',
       '220.113.20.123',
       '220.113.20.124',
       '220.113.20.127',
       '220.113.20.128',
       '220.113.20.129',
       '220.113.20.131']
usr = 'm'
port = 22
pwd = 'softraid'


def start_all():
    servers = dict()
    servers['object-server'] = []
    servers['container-server'] = []
    servers['account-server'] = []
    servers['proxy-server'] = []
    server_names = ['object-server', 'container-server', 'account-server']
    for ip in ips:
        for server in server_names:
            print 'starting %s on %s' % (server, ip)
            cmd = '/home/m/mjwtom/bin/python /home/m/mjwtom/swift/bin/swift-%s ' \
                  '/home/m/mjwtom/swift/dedupe/swift/%s.conf' % server
            args = (usr, ip, port, pwd, cmd)
            servers[server].append(Thread(target=run_cmd, args=args))
    cmd = '/home/m/mjwtom/bin/python /home/m/mjwtom/swift/bin/swift-proxy-server ' \
          '/home/m/mjwtom/swift/dedupe/swift/proxy-server.conf'
    args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)
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
    elif run_type == 'kill':
        kill_all()
