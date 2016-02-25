#!/usr/bin/env python
# -*- coding: utf-8 -*-


from ssh import SSH
from subprocess import Popen
import time


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


def update_python():
    for ip in ips:
        print ip
        cmd = 'ssh m@%s cat /etc/yum.repos.d/CentOS-Base.repo | grep nankai' % ip
        p = Popen(cmd, shell=True)
        outdata, errdata = p.communicate()
        if '[y/N]' in outdata:
            p.communicate(input='Y\n')


def install_software(softwares=None):
    for ip in ips:
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
            if 'Total download' in l:
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

def simple_cmd_allnode(cmds=None):
    for ip in ips:
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


def share_code():
    src_dir='/home/m/mjwtom/'
    dst_dir = '/home/m/mjwtom/'

    for ip in ips:
        print ip
        time.sleep(1)
        client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
        client.connect()
        client.transport(src_dir, dst_dir, 'put', True)


softwares = ['curl',
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

#share_code()
'''
cmds = ['sudo -k cp -r /home/m/mjwtom/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo']
ips = ['222.30.48.9']
port = 9150
'''



cmds = ['sudo -k yum clean all',
        'sudo -k yum makecache']

install_software(softwares)

#simple_cmd_allnode(cmds)