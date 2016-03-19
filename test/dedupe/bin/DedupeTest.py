#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-
import os
from swift.dedupe.time import time, time_diff
import subprocess
from nodes import proxy_ip
from test.dedupe.ssh import SSH
import pickle
from random import randint


class DeduplicationTest(object):
    def __init__(self, log_file):
        self.name = 'deduplication test'
        if log_file:
            self.log = open(log_file, 'a')
        else:
            self.log = None

    def __del__(self):
        if self.log:
            self.log.close()

    def info(self, info):
        if self.log:
            self.log.write(info+'\n')
            self.log.flush()

    def check_node(self, nodes):
        for node in nodes:
            print 'checking node %s......:' % node
            pass

    def upload(self, path):
        cmd = ['/home/mjw/bin/swift',
               '-A',
               'http://%s:8080/auth/v1.0' % proxy_ip,
               '-U',
               'test:tester',
               '-K',
               'testing',
               'upload',
               'mjwtom',
               path]
        print 'uploading %s\n' % path
        start = time()
        try:
            ret = subprocess.call(cmd)
            if ret == 0:
                print 'success upload file\n'
            else:
                print 'fail to upload file\n'
        except Exception as e:
            print e
        end = time()
        time_used = time_diff(start, end)
        size = os.path.getsize(path)
        throughput = size/time_used
        print 'upload %s, size %d, time %f, throughput %f\n\n' % (path, size, time_used, throughput)
        info = dict(
            file = path,
            size = size,
            time = time_used,
            throughput = throughput
        )
        return info

    def upload_dir(self, path):
        if os.path.isfile(path):
            self.upload(path)
        elif os.path.isdir(path):
            for file in os.listdir(path):
                subpath = os.path.join(path, file)
                self.upload(subpath)
        else:
            print 'wrong'
    '''
    def fetch_upload(self, files, tmp_dir):
        upload_info = []
        upload_files = []
        for file in files:
            print 'fetching %s' % file
            client = SSH(file_server_usr, file_server, file_server_port, file_server_pwd)
            pre, filename = os.path.split(file)
            local_path = os.path.join(tmp_dir, filename)
            start = time()
            client.transport(local_path, file, 'get')
            end = time()
            time_used = time_diff(start, end)
            size = os.path.getsize(local_path)
            info = 'fech %s to %s, size %d, time %f, throughput %f\n' % (file, local_path, size, time_used, size/time_used)
            print info
            self.info(info)
            info = self.upload(local_path)
            upload_files.append(local_path)
            upload_info.append(info)
            cmd = ['rm',
                   '-rf',
                   local_path]
            ret = subprocess.call(cmd)
            if ret == 0:
                print 'successfully remove file %s' % local_path
            else:
                print 'failed to remove file %s' % local_path
        return upload_info, upload_files
        '''

    def uploads(self, files):
        results = []
        for f in files:
            results.append(self.upload(f))
        return results, files

    def download(self, file):
        cmd =['/home/mjw/bin/swift',
                  '-A',
                  'http://%s:8080/auth/v1.0' % proxy_ip,
                  '-U',
                  'test:tester',
                  '-K',
                  'testing',
                  'download',
              'mjwtom',
                  file]
        print 'downloading file %s\n' % file
        start = time()
        try:
            ret = subprocess.call(cmd)
            if ret == 0:
                print 'successfully download file %s\n' % file
            else:
                print 'failed to download file %s\n' % file
        except Exception as e:
            print e
        end = time()
        time_used = time_diff(start, end)
        size = os.path.getsize(file)
        throughput = size/time_used
        info = 'download %s, size %d, time %f, throughput %f\n\n' % (file, size, time_used, throughput)
        print info
        self.info(info)
        info = dict(
            file = file,
            size = size,
            time = time_used,
            throughput = throughput
        )
        path = os.getcwd()
        path = os.path.join(path, file)
        cmd = ['rm',
               '-rf',
               path]
        print 'removing file %s' % path
        ret = subprocess.call(cmd)
        if ret == 0:
            print 'successfully remove file %s\n\n' % file
        else:
            print 'failed to remove file %s\n\n' % file
        return info

    def sequential_download(self, files):
        download_files = []
        download_info = []
        for file in files:
            download_info.append(self.download(file))
            download_files.append(file)
        return download_info, download_files

    def random_download(self, files):
        download_files = []
        download_info = []
        rnd_files = files[:]
        l = len(rnd_files)
        while l > 0:
            index = randint(0, l-1)
            file = rnd_files.pop(index)
            download_info.append(self.download(file))
            download_files.append(file)
            l = len(rnd_files)
        return download_info, download_files

    def scan_dir(self, path, pickle_file, min_size=0):
        if not os.path.exists(path):
            print 'path does not exist'

        def deep_scan(path, files, min_size):
            if os.path.isfile(path):
                size = os.path.getsize(path)
                if size >= min_size:
                    files.append(path)
            else:
                if os.path.isdir(path):
                    for file in os.listdir(path):
                        subpath = os.path.join(path, file)
                        deep_scan(subpath, files, min_size)

        files = []
        deep_scan(path, files, min_size)
        out = open(pickle_file, 'wb')
        pickle.dump(files, out)
        out.close()

    def get_files(self, pickle_file):
        try:
            inf = open(pickle_file, 'rb')
            files = pickle.load(inf)
            inf.close()
        except IOError as e:
            print 'can not open the pickle file storing the positions'
            return None
        return files

    def print_files(self, files):
        for file in files:
            print file