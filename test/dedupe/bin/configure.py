#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-


from threading import Thread
import sys
from test.dedupe.ssh import SSH, run_cmds, uploads, run_cmd, upload
from nodes import ips, pwd, usr, port
from nodes import client_ip, client_usr, client_port, client_pwd


def install_software(usr='root', ip='127.0.0.1', port=22, pwd=None, softwares=None):
    print ip

    client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
    cmd = 'sudo -k rpm -Uvh http://pkgs.repoforge.org/rpmforge-release/rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm'
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


def thread_cmds(cmds):
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, cmds)
        threads.append(Thread(target=run_cmds, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_reboot(multi_thread=False):
    if multi_thread:
        threads = []
        for ip in ips:
            args = (usr, ip, port, pwd, 'sudo -k reboot')
            threads.append(Thread(target=run_cmd, args=args))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        for ip in ips:
            print 'rebooting % s' % ip
            run_cmd(usr, ip, port, pwd, 'sudo -k reboot')


def thread_share_code(tasks=None, multi_thread=False):
    tasks = [('/home/mjwtom/swift', '/home/m/mjwtom/swift')]
    if multi_thread:
        threads = []
        for ip in ips:
            args = (usr, ip, port, pwd, tasks)
            threads.append(Thread(target=uploads, args=args))
        tasks = [('/home/mjwtom/swift', '/home/mjw/swift')]
        args = (client_usr, client_ip, client_port, client_pwd, tasks)
        threads.append(Thread(target=uploads, args=args))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        for ip in ips:
            print 'uploading %s' % ip
            uploads(usr, ip, port, pwd, tasks)
        print 'uploading code to client'
        tasks = [('/home/mjwtom/swift', '/home/mjw/swift')]
        uploads(client_usr, client_ip, client_port, client_pwd, tasks)


def thread_share_ring(multi_thread=False):
    if multi_thread:
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
    else:
        src = '/etc/swift/'
        dst = '/home/m/mjwtom/swift-etc'
        for ip in ips:
            print 'uploading %s' % ip
            upload(usr, ip, port, pwd, src, dst)
        cmds = ['sudo -k rm -rf /etc/swift',
                'sudo -k mv /home/m/mjwtom/swift-etc /etc/swift -f']
        for ip in ips:
            print 'excuting on %s' % ip
            run_cmds(usr, ip, port, pwd, cmds)


def thread_install(multi_thread=False):
    if multi_thread:
        threads = []
        cmd = 'cd /home/m/mjwtom/swift; /home/m/mjwtom/bin/python setup.py develop;'
        for ip in ips:
            args = (usr, ip, port, pwd, cmd)
            threads.append(Thread(target=run_cmd, args=args))

        cmd = 'cd /home/mjwtom/swift; /home/mjwtom/bin/python setup.py develop;'
        args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)
        threads.append(Thread(target=run_cmd, args=args))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        cmd = 'cd /home/m/mjwtom/swift; /home/m/mjwtom/bin/python setup.py develop;'
        for ip in ips:
            print 'run cmd on %s' % ip
            run_cmd(usr, ip, port, pwd, cmd)

        cmd = 'cd /home/mjwtom/swift; /home/mjwtom/bin/python setup.py develop;'
        print 'run cmd on 127.0.0.1'
        run_cmd('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)


def make_rings():
    cmd = 'sudo -k /home/mjwtom/bin/python /home/mjwtom/swift/test/dedupe/bin/remakerings.py'
    run_cmd('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)


def replace_etc_swift():
    cmd = 'sudo -k cp -rf /home/mjwtom/swift/test/dedupe/swift /etc/'
    run_cmd('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)


def setup_log():
    cmds = ['sudo -k service rsyslog stop',
            'sudo -k rm -rf /var/log/swift/*'
            'sudo -k mkdir -p /var/log/swift/hourly',
            'sudo -k chown -R root:adm /var/log/swift',
            'sudo -k chmod -R g+w /var/log/swift',
            'sudo -k cp -f /home/m/mjwtom/swift/test/dedupe/rsyslog.d/10-swift.conf /etc/rsyslog.d/',
            'sudo -k service rsyslog restart']
    run_cmds('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)


def thread_start_services(multi_thread=False):
    cmds = ['sudo -k service rsync restart',
            'sudo -k service memcached restart',
            'sudo -k service rsyslog restart',
            'sudo -k chkconfig rsync on',
            'sudo -k chkconfig memcached on',
            'sudo -k chkconfig rsyslog on']
    if multi_thread:
        threads = []
        for ip in ips:
            args = (usr, ip, port, pwd, cmds)
            threads.append(Thread(target=run_cmds, args=args))

        args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)
        threads.append(Thread(target=run_cmds, args=args))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        for ip in ips:
            run_cmds(usr, ip, port, pwd, cmds)
        run_cmds('mjwtom', '127.0.0.1', 22, 'missing1988', cmds)


def thread_file_system():
    threads = []
    cmd = 'sudo -k cat /etc/fstab'
    for ip in ips:
        args = (usr, ip, port, pwd, cmd)
        threads.append(Thread(target=run_cmd, args=args))

    args = ('mjwtom', '127.0.0.1', 22, 'missing1988', cmd)
    threads.append(Thread(target=run_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

softwares_swift = ['curl',
                    'gcc',
                    'gcc-c++'
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
                    'python-mock',
                    'memcached']

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

cmd_mkdir_mjwtom = ['mkdir /home/m/mjwtom']

tasks_before = [('/home/mjwtom/swift', '/home/m/mjwtom/swift'),
         ('/home/mjwtom/Python-2.7.11.tgz', '/home/m/mjwtom/Python-2.7.11.tgz'),
         ('/home/mjwtom/setuptools-20.2.1.tar.gz', '/home/m/mjwtom/setuptools-20.2.1.tar.gz'),
         ('/home/mjwtom/pip-8.0.2.tar.gz', '/home/m/mjwtom/pip-8.0.2.tar.gz')]




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


def all_cmds():
    thread_install_software(softwares_swift)
    thread_install_software(softwares_python)
    thread_cmds(cmd_mkdir_mjwtom)
    thread_share_code(tasks_before)
    thread_cmds(cmds_python)
    replace_etc_swift()
    thread_install()
    make_rings()
    thread_share_ring()



if __name__ == '__main__':
    if len(sys.argv) < 2:
        exit()
    if 'all' in sys.argv:
        all_cmds()
        exit()
    if 'file' in sys.argv:
        thread_file_system()
    if 'code' in sys.argv:
        thread_share_code(multi_thread=True)
    if 'replace_etc' in sys.argv:
        replace_etc_swift()
    if 'service' in sys.argv:
        thread_start_services()
    if 'install' in sys.argv:
        thread_install()
    if 'make_ring' in sys.argv:
        make_rings()
    if 'share_ring' in sys.argv:
        thread_share_ring()
    if 'setup_log' in sys.argv:
        setup_log()
    if 'environment' in sys.argv:
        thread_cmds(cmds_python)
    if 'reboot' in sys.argv:
        thread_reboot()
