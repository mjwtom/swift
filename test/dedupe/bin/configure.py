#!/usr/bin/env python
# -*- coding: utf-8 -*-


from threading import Thread
import sys
from test.dedupe.ssh import SSH, run_cmds, uploads, run_cmd, upload


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


def install_software(usr='root', ip='127.0.0.1', port=22, pwd=None, softwares=None):
    print ip
    client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
    cmd = 'sudo -k yum update'
    stdin, stdout, stderr = client.execute(cmd+'\n', True, old_pty=True)
    if cmd.startswith('sudo'):
        stdin.write(pwd+'\n')
        stdin.flush()
    for l in stdout:
        print 'stdout: %s' % l.strip()
        if 'Total' in l:
            stdin.write('Y\n')
            stdin.flush()
    for l in stderr:
        print 'stderr: %s' % l.strip()
        return

    for software in softwares:
        cmd = 'sudo -k yum install %s' % software
        stdin, stdout, stderr = client.execute(cmd+'\n', True, old_pty=True)
        if cmd.startswith('sudo'):
            stdin.write(pwd+'\n')
            stdin.flush()
        for l in stdout:
            print 'stdout: %s' % l.strip()
            if 'Installed size' in l:
                stdin.write('Y\n')
                stdin.flush()
        for l in stderr:
            print 'stderr: %s' % l.strip()
            return


def thread_install_software(softwares):
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, softwares)
        threads.append(Thread(target=install_software, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_cmd(cmds):
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, cmds)
        threads.append(Thread(target=run_cmds, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_reboot():
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, 'sudo -k reboot')
        threads.append(Thread(target=run_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_share_code(tasks=None):
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, tasks)
        threads.append(Thread(target=uploads, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_share_ring():
    threads = []
    src = '/etc/swift/'
    dst = '/home/m/mjwtom/swift-etc'
    for ip in ips:
        args = (usr, ip, port, pwd, src, dst)
        threads.append(Thread(target=upload, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    cmds = ['sudo -k rm -rf /etc/swift',
            'sudo -k mv /home/m/mjwtom/swift-etc /etc/swift -f']
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, cmds)
        threads.append(Thread(target=run_cmds, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_install():
    threads = []
    cmd = 'cd /home/m/mjwtom/swift; /home/m/mjwtom/bin/python setup.py develop;'
    for ip in ips:
        args = (usr, ip, port, pwd, cmd)
        threads.append(Thread(target=run_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def make_rings():
    cmd = 'sudo -k /home/mjwtom/bin/python /home/mjwtom/swift/test/dedupe/bin/remakerings.py'
    run_cmd('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)


def copy_etc():
    cmd = 'sudo -k cp -rf /home/mjwtom/swift/test/dedupe/swift /etc/'
    run_cmd('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)


softwares_swift = ['curl',
             'gcc',
             'memcached',
             'rsync',
             'sqlite',
             'xfsprogs',
             'git-core',
             'libffi-devel',
             'xinetd',
             'liberasurecode-devel',
             'python-setuptools',
             'python-coverage'
             'python-devel python-nose',
             'pyxattr',
             'python-eventlet',
             'python-greenlet',
             'python-paste-deploy',
             'python-netifaces',
             'python-pip',
             'python-dns',
             'python-mock']

softwares_python = ['zlib-devel',
             'bzip2-devel',
             'openssl-devel',
             'ncurses-devel',
             'sqlite-devel']


'''
cmds = ['sudo -k cp -r /home/m/mjwtom/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo']
ips = ['222.30.48.9']
port = 9150
'''

tasks_before = [('/home/mjwtom/swift', '/home/m/mjwtom/swift'),
         ('/home/mjwtom/Python-2.7.11.tgz', '/home/m/mjwtom/Python-2.7.11.tgz'),
         ('/home/mjwtom/setuptools-20.2.1.tar.gz', '/home/m/mjwtom/setuptools-20.2.1.tar.gz'),
         ('/home/mjwtom/pip-8.0.2.tar.gz', '/home/m/mjwtom/pip-8.0.2.tar.gz')]

tasks = [('/home/mjwtom/swift', '/home/m/mjwtom/swift')]




cmds_python = ['rm /home/m/mjwtom/Python-2.7.11/ -rf',
               'rm /home/m/mjwtom/install/ -rf',
               'rm /home/m/mjwtom/pip-8.0.2/ -rf',
               'rm /home/m/jwtom/swift-data -rf',
               'rm /home/m/jwtom/bin -rf',
               'rm /home/m/mjwtom/setuptools-20.2.1/ -rf',
               'cd /home/m/mjwtom/; tar -zxvf Python-2.7.11.tgz;',
               'cd /home/m/mjwtom/; tar -zxvf setuptools-20.2.1.tar.gz;',
               'cd /home/m/mjwtom/; tar -zxvf pip-8.0.2.tar.gz;',
               'cd /home/m/mjwtom/Python-2.7.11; ./configure --prefix=/home/m/mjwtom/install/python; make install;',
               'cd /home/m/mjwtom/setuptools-20.2.1;  /home/m/mjwtom/install/python/bin/python setup.py install;',
               'cd /home/m/mjwtom/pip-8.0.2;  /home/m/mjwtom/install/python/bin/python setup.py install;',
               'mkdir /home/m/mjwtom/swift-data',
               'mkdir /home/m/mjwtom/swift-data/sdb1',
               'mkdir /home/m/mjwtom/bin',
               'ln -s /home/m/mjwtom/install/python/bin/python /home/m/mjwtom/bin/python',
               'ln -s /home/m/mjwtom/install/python/bin/pip /home/m/mjwtom/bin/pip',
               'cd /home/m/mjwtom/swift; /home/m/mjwtom/bin/pip uninstall -r requirements.txt',
               'cd /home/m/mjwtom/swift; /home/m/mjwtom/bin/pip install -r requirements.txt',
               'cd /home/m/mjwtom/swift; /home/m/mjwtom/bin/python setup.py develop']


if __name__ == '__main__':
    if len(sys.argv) < 2:
        exit()
    if 'code' in sys.argv:
        thread_share_code(tasks)
    if 'copy_etc' in sys.argv:
        copy_etc()
    if 'make_ring' in sys.argv:
        make_rings()
    if 'share_ring' in sys.argv:
        thread_share_ring()
    if 'install' in sys.argv:
        thread_install()
    if 'environment' in sys.argv:
        thread_cmd(cmds_python)
    if 'reboot' in sys.argv:
        thread_reboot()
