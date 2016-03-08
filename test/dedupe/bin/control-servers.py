#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-
import sys
from test.dedupe.ssh import run_cmd, run_cmds
from threading import Thread
from nodes import ips, usr, pwd, port


def start_all(muti_thread=False):
    servers = dict()
    servers['object-server'] = []
    servers['container-server'] = []
    servers['account-server'] = []
    servers['proxy-server'] = []
    server_names = ['object-server', 'container-server', 'account-server']
    for ip in ips:
        for server in server_names:
            print 'starting %s on %s' % (server, ip)
            cmds = ['sudo -k service memcached restart',
                   '/home/m/mjwtom/bin/python /home/m/mjwtom/swift/bin/swift-%s ' \
                  '/home/m/mjwtom/swift/test/dedupe/swift/%s.conf' % (server, server)]
            if muti_thread:
                args = (usr, ip, port, pwd, cmds)
                servers[server].append(Thread(target=run_cmds, args=args))
            else:
                run_cmds(usr, ip, port, pwd, cmds)
    print 'starting %s on %s' % ('proxy-server', '127.0.0.1')
    cmds = ['sudo -k service memcached restart',
           '/home/mjwtom/bin/python /home/mjwtom/swift/bin/swift-proxy-server ' \
          '/home/mjwtom/swift/test/dedupe/swift/proxy-server.conf']
    if muti_thread:
        args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)
        servers['proxy-server'].append(Thread(target=run_cmds, args=args))
    else:
        run_cmds('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)
    if muti_thread:
        threads = [server for server_name, serverlist in servers.items() for server in serverlist]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


def kill_all(multi_thread=False):
    servers = ['object-server', 'container-server', 'account-server', 'proxy-server']
    cmds = []
    for name in servers:
        cmd = 'ps -aux | grep swift-%s | grep -v grep | cut -c 9-15 | xargs kill -s 9' % name
        cmds.append(cmd)
    if multi_thread:
        threads = []
        for ip in ips:
            print 'killing the servers in node %s' % ip
            args = (usr, ip, port, pwd, cmds)
            threads.append(Thread(target=run_cmds, args=args))
        print 'killing the servers in node 127.0.0.1'
        args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)
        threads.append(Thread(target=run_cmds, args=args))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        for ip in ips:
            print 'killing the servers in node %s' % ip
            run_cmds(usr, ip, port, pwd, cmds)
        print 'killing the servers in node 127.0.0.1'
        run_cmds('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)


def clean_data(multi_thread=False):
    cmd = 'rm -rf /home/m/mjwtom/swift-data/sdb1/*'
    if multi_thread:
        threads = []
        for ip in ips:
            print 'removing the data in node %s' % ip
            args = (usr, ip, port, pwd, cmd)
            threads.append(Thread(target=run_cmd, args=args))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        for ip in ips:
            print 'removing the data in node %s' % ip
            run_cmd(usr, ip, port, pwd, cmd)


def reset_all():
    kill_all()
    clean_data()
    start_all()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        exit()
    run_type = sys.argv[1]
    if run_type == 'start':
        start_all()
    elif run_type == 'kill':
        kill_all()
    elif run_type == 'reset':
        reset_all()
