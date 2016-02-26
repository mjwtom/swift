#!/usr/bin/env python
# -*- coding: utf-8 -*-


from ssh import SSH
from subprocess import Popen
import time
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


def install_software(usr='root', ip='127.0.0.1', port=22, pwd=None, softwares=None):
    print ip
    client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
    client.connect()
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


def reboot(usr='root', ip='127.0.0.1', port=22, pwd=None):
    print ip
    client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
    client.connect()
    cmd = 'sudo -k reboot'
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


def simple_cmd(usr='root', ip='127.0.0.1', port=22, pwd=None, cmds=None):
    print ip
    client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
    client.connect()
    for cmd in cmds:
        print cmd
        stdin, stdout, stderr = client.execute(cmd, True, old_pty=True)
        if cmd.startswith('sudo'):
            stdin.write(pwd+'\n')
            stdin.flush()
        for l in stdout:
            print 'stdout: %s' % l.strip()
        for l in stderr:
            print 'stderr: %s' % l.strip()
            return


def share_code(usr='root', ip='127.0.0.1', port=22, pwd=None, tasks=None):
    print ip
    for src, dst in tasks:
        client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
        client.connect()
        client.transport(src, dst, 'put', True)


def thread_install_software(softwares):
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, softwares)
        threads.append(Thread(target=install_software, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_cmd(cmd):
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, cmd)
        threads.append(Thread(target=simple_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_reboot():
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd)
        threads.append(Thread(target=reboot, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_share_code(tasks=None):
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, tasks)
        threads.append(Thread(target=share_code, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


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

tasks = [('/home/mjwtom/swift', '/home/m/mjwtom/swift'),
         ('/home/mjwtom/Python-2.7.11.tgz', '/home/m/mjwtom/Python-2.7.11.tgz'),
         ('/home/mjwtom/setuptools-20.2.1.tar.gz', '/home/m/mjwtom/setuptools-20.2.1.tar.gz'),
         ('/home/mjwtom/pip-8.0.2.tar.gz', '/home/m/mjwtom/pip-8.0.2.tar.gz')]



cmds = ['rm /home/m/mjwtom/Python-2.7.11/ -rf',
        'rm /home/m/mjwtom/install/ -rf',
        'rm /home/m/mjwtom/pip-8.0.2/ -rf',
        'rm /home/m/mjwtom/setuptools-20.2.1/ -rf',
        'cd /home/m/mjwtom/; tar -zxvf Python-2.7.11.tgz;',
        'cd /home/m/mjwtom/; tar -zxvf setuptools-20.2.1.tar.gz;',
        'cd /home/m/mjwtom/; tar -zxvf pip-8.0.2.tar.gz;',
        'cd /home/m/mjwtom/Python-2.7.11; ./configure --prefix=/home/m/mjwtom/install/python; make install;',
        'cd /home/m/mjwtom/setuptools-20.2.1;  /home/m/mjwtom/install/python/bin/python setup.py install;',
        'cd /home/m/mjwtom/pip-8.0.2;  /home/m/mjwtom/install/python/bin/python setup.py install;']

thread_install_software(softwares_python)

thread_share_code(tasks)

thread_cmd(cmds)

# thread_reboot()
