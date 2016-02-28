import paramiko
import os
import stat


class SSH(object):
    def __init__(self, usr='root', ip='127.0.0.1', port=22, pwd='', connect=True, logfile='/tmp/dedupe-ssh-log.txt'):
        self.usr = usr
        self.pwd = pwd
        self.ip = ip
        self.port = port
        self.logfile = logfile
        self.client = None
        if connect:
            self.connect()

    def connect(self):
        if not self.client:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(hostname=self.ip,
                                port=self.port,
                                username=self.usr,
                                password=self.pwd)

    def close(self):
        if self.client:
            self.client.close()
            self.client = None

    def execute(self, cmd= None, get_pty=False, old_pty=False):
        if old_pty:
            trans = self.client.get_transport()
            session = trans.open_session()
            session.set_combine_stderr(True)
            cmd = cmd.strip()
            if cmd.startswith('sudo') and (get_pty==False):
                print 'warning: when use sudo, get_pty should be set'
            if get_pty:
                session.get_pty()
            session.exec_command(cmd)
            stdin = session.makefile('wb', -1)
            stdout = session.makefile('rb', -1)
            #you have to check if you really need to send password here
            return stdin, stdout, ''
        else:
            return self.client.exec_command(cmd, get_pty=get_pty)

    def __del__(self):
        self.close()

    def transport(self, local, remote, method='put', rm_old=False):

        def recur_put(sftp, local, remote):
            if os.path.isfile(local):
                print 'uploading %s' % local
                sftp.put(local, remote)
            elif os.path.isdir(local):
                try:
                    print 'making directory %s' % local
                    sftp.mkdir(remote)
                except IOError as e:
                    print '(assuming ', remote, 'exists)', e
                files = os.listdir(local)
                for f in files:
                    local_subpath = os.path.join(local, f)
                    remote_subpath = os.path.join(remote, f)
                    recur_put(sftp, local_subpath, remote_subpath)

        def rmtree(sftp, remotepath):
            remote_tate = sftp.lstat(remotepath)
            if stat.S_ISDIR(remote_tate.st_mode):
                for file in sftp.listdir(remotepath):
                    subpath = remotepath + '/' + file
                    print 'removing directory %s' % subpath
                    rmtree(sftp, subpath)
                sftp.rmdir(remotepath)
            else:
                print 'removing file %s' % remotepath
                sftp.remove(remotepath)

        if not os.path.exists(local):
            return
        trans = self.client.get_transport()
        sftp = paramiko.SFTPClient.from_transport(trans)
        if method == 'put':
            try:
                if rm_old:
                    rmtree(sftp, remote)
                    print 'remove the old directory'
                else:
                    print 'not remove the original directory'
            except IOError as e:
                print '(assuming ', remote, 'does not exists)', e
            recur_put(sftp, local, remote)
        elif method == 'get':
            files = sftp.listdir(remote)
            for f in files:
                sftp.get(os.path.join(remote, f), os.path.join(local, f))
        trans.close()


def run_cmd(usr='root', ip='127.0.0.1', port=22, pwd=None, cmd=None):
    client = SSH(usr=usr, ip=ip, pwd=pwd, port=port)
    cmd = cmd.strip()
    if cmd.startswith('sudo'):
        stdin, stdout, stderr = client.execute(cmd, True, old_pty=True)
        stdin.write(pwd+'\n')
        stdin.flush()
    else:
        stdin, stdout, stderr = client.execute(cmd)
    for l in stdout:
        print '%s stdout: %s' % (ip, l.strip())
    for l in stderr:
        print '%s stderr: %s' % (ip, l.strip())


def run_cmds(usr='root', ip='127.0.0.1', port=22, pwd=None, cmds=None):
    for cmd in cmds:
        run_cmd(usr, ip, port, pwd, cmd)


def upload(usr='root', ip='127.0.0.1', port=22, pwd=None, src=None, dst=None):
    client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
    client.transport(src, dst, 'put', True)


def uploads(usr='root', ip='127.0.0.1', port=22, pwd=None, tasks=None):
    for src, dst in tasks:
        upload(usr, ip,  port, pwd, src, dst)