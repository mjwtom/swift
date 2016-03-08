import paramiko
import os
from stat import S_ISDIR, S_ISREG


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

    def execute(self, cmd=None, get_pty=False, old_pty=False):
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
            try:
                stat = os.stat(local)
            except IOError as e:
                print 'remote file does not exist', e
                return
            mode = stat.st_mode & 0777
            if os.path.isfile(local):
                print 'pushing %s' % local
                try:
                    sftp.put(local, remote)
                    sftp.chmod(remote, mode)
                except IOError as e:
                    print '(assuming ', remote, 'parent directory does not exist)', e
            elif os.path.isdir(local):
                try:
                    print 'making directory %s' % remote
                    sftp.mkdir(remote, mode=mode)
                except IOError as e:
                    print '(assuming ', remote, 'parent directory exists)', e
                for file in os.listdir(local):
                    local_subpath = os.path.join(local, file)
                    remote_subpath = os.path.join(remote, file)
                    recur_put(sftp, local_subpath, remote_subpath)

        def recur_get(sftp, local, remote):
            try:
                stat = sftp.lstat(remote)
            except IOError as e:
                print 'remote file does not exist', e
                return
            mode = stat.st_mode & 0777
            if S_ISREG(stat.st_mode):
                print 'pulling %s' % remote
                try:
                    sftp.get(remote, local)
                    os.chmod(local, mode)
                except IOError as e:
                    print 'assuming ', local, 'prarent directory does not exist', e
            elif S_ISDIR(stat.st_mode):
                try:
                    print 'making directory %s' % local
                    os.mkdir(local, mode)
                except IOError as e:
                    print '(assuming ', local, 'parent directory exists)', e
                for file in sftp.listdir(remote):
                    local_subpath = os.path.join(local, file)
                    remote_subpath = os.path.join(remote, file)
                    recur_get(sftp, local_subpath, remote_subpath)


        def rmtree(sftp, remotepath):
            remote_tate = sftp.lstat(remotepath)
            if S_ISDIR(remote_tate.st_mode):
                for file in sftp.listdir(remotepath):
                    subpath = remotepath + '/' + file
                    rmtree(sftp, subpath)
                print 'removing directory %s' % remotepath
                sftp.rmdir(remotepath)
            elif S_ISREG(remote_tate.st_mode):
                print 'removing file %s' % remotepath
                sftp.remove(remotepath)
            else:
                print 'neither directory nor file % s' % remotepath

        def rmtree_local(localpath):
            if os.path.isdir(localpath):
                for file in os.listdir(localpath):
                    subpath = os.path.join(localpath, file)
                    rmtree_local(subpath)
                print 'removing directory %s' % localpath
                os.remove(localpath)
            elif os.path.isfile(localpath):
                print 'removing file %s' % localpath
                os.remove(localpath)
            else:
                print 'neither directory nor file %s' % localpath

        trans = self.client.get_transport()
        sftp = paramiko.SFTPClient.from_transport(trans)
        if method == 'put':
            try:
                if rm_old:
                    rmtree(sftp, remote)
                    print 'remove the old files'
                else:
                    print 'not remove the old files'
            except IOError as e:
                print '(assuming ', remote, 'does not exists)', e
            recur_put(sftp, local, remote)
        elif method == 'get':
            try:
                if rm_old:
                    rmtree_local(local)
                    print 'remove the old files'
                else:
                    print 'not remove the old files'
            except IOError as e:
                print '(assuming ', remote, 'does not exists)', e
            recur_get(sftp, local, remote)
        trans.close()


def run_cmd(usr='root', ip='127.0.0.1', port=22, pwd=None, cmd=None):
    try:
        client = SSH(usr=usr, ip=ip, pwd=pwd, port=port)
        cmd = cmd.strip()
        if cmd.startswith('sudo'):
            stdin, stdout, stderr = client.execute(cmd, True, old_pty=True)
            stdin.write(pwd+'\n')
            stdin.flush()
        else:
            stdin, stdout, stderr = client.execute(cmd)
        try:
            for l in stdout:
                print '%s stdout: %s' % (ip, l.strip())
            for l in stderr:
                print '%s stderr: %s' % (ip, l.strip())
        except Exception as e:
            print 'cannot read the output %s on %s, error:' % (cmd, ip), e
    except Exception as e:
        print 'cannot execute the command %s on %s, error:' % (cmd, ip), e


def run_cmds(usr='root', ip='127.0.0.1', port=22, pwd=None, cmds=None):
    for cmd in cmds:
        run_cmd(usr, ip, port, pwd, cmd)


def upload(usr='root', ip='127.0.0.1', port=22, pwd=None, src=None, dst=None):
    try:
        client = SSH(usr=usr, ip=ip, port=port, pwd=pwd)
        client.transport(src, dst, 'put', True)
    except Exception as e:
        print 'cannot transport the file %s to %s, error:' % (src, ip), e


def uploads(usr='root', ip='127.0.0.1', port=22, pwd=None, tasks=None):
    for src, dst in tasks:
        upload(usr, ip,  port, pwd, src, dst)