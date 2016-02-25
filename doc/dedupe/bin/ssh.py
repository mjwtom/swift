import paramiko
import os
import stat


class SSH(object):
    def __init__(self, usr='root', ip='127.0.0.1', port=22, pwd='', logfile='/tmp/dedupe-ssh-log.txt'):
        self.usr = usr
        self.pwd = pwd
        self.ip = ip
        self.port = port
        self.logfile = logfile
        self.client = None

    def connect(self, is_cmd=True):
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

    def execute(self, cmd= None, get_pty=True, old_pty=False):
        if old_pty:
            trans = self.client.get_transport()
            session = trans.open_session()
            session.set_combine_stderr(True)
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
                sftp.put(local, os.path.join(remote, local))
            elif os.path.isdir(local):
                try:
                    sftp.mkdir(remote)
                except IOError as e:
                    print '(assuming ', remote, 'exists)', e
                files = os.listdir(local)
                for f in files:
                    local_subpath = os.path.join(local, f)
                    remomte_subpath = os.path.join(remote, f)
                    if os.path.isfile(local_subpath):
                        print 'uploading file %s' % remomte_subpath
                        sftp.put(local_subpath, remomte_subpath)
                    elif os.path.isdir(local_subpath):
                        recur_put(sftp, local_subpath, remomte_subpath)

        def rmtree(sftp, remotepath, level=0):
            for f in sftp.listdir_attr(remotepath):
                rpath = os.path.join(remotepath, f.filename)
                if stat.S_ISDIR(f.st_mode):
                    rmtree(sftp, rpath, level=(level + 1))
                else:
                    rpath = os.path.join(remotepath, f.filename)
                    print('removing %s%s' % ('    ' * level, rpath))
                    sftp.remove(rpath)
            print('removing %s%s' % ('    ' * level, remotepath))
            sftp.rmdir(remotepath)

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