#!/usr/bin/python
import sys
from test.dedupe.ssh import run_cmd, run_cmds
from threading import Thread


ips = ['220.113.20.142',
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
            cmd = ['sudo -k service iptables stop',
                   '/home/m/mjwtom/bin/python /home/m/mjwtom/swift/bin/swift-%s ' \
                  '/home/m/mjwtom/swift/test/dedupe/swift/%s.conf' % (server, server)]
            args = (usr, ip, port, pwd, cmd)
            servers[server].append(Thread(target=run_cmds, args=args))
    cmd = ['sudo -k service iptables stop',
           '/home/mjwtom/bin/python /home/mjwtom/swift/bin/swift-proxy-server ' \
          '/home/mjwtom/swift/test/dedupe/swift/proxy-server.conf']
    args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)
    servers['proxy-server'].append(Thread(target=run_cmds, args=args))
    threads = [server for server_name, serverlist in servers.items() for server in serverlist]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def kill_all():
    threads = []
    servers = ['object-server', 'container-server', 'account-server', 'proxy-server']
    cmds = []
    for name in servers:
        cmd = 'ps -aux | grep swift-%s | grep -v grep | cut -c 9-15 | xargs kill -s 9' % name
        cmds.append(cmd)
    for ip in ips:
        print 'killing the servers in node %s' % ip
        args = (usr, ip, port, pwd, cmds)
        threads.append(Thread(target=run_cmds, args=args))
    args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)
    threads.append(Thread(target=run_cmds, args=args))
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
