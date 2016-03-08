#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-
import os
from swift.dedupe.time import time, time_diff
import subprocess
from nodes import proxy_ip
from test.dedupe.ssh import SSH
from nodes import file_server, file_server_usr, file_server_port, file_server_pwd
import pickle
from random import randint


class DeduplicationTest(object):
    def __init__(self, log_file):
        self.name = 'deduplication test'
        self.uploads = []
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
        if os.path.isfile(path):
            cmd = ['/home/m/mjwtom/bin/swift',
                   '-A',
                   'http://%s:8080/auth/v1.0' % proxy_ip,
                   '-U',
                   'test:tester',
                   '-K',
                   'testing',
                   'upload',
                   'mjwtom',
                   path]
            dedupe_start = time()
            subprocess.call(cmd)
            dedupe_end = time()
            time_used = time_diff(dedupe_start, dedupe_end)
            size = os.path.getsize(file)
            throughput = size/time_used
            info = dict(
                file = path,
                size = size,
                time = time_used,
                throughput = throughput
            )
            self.uploads.append(info)
        elif os.path.isdir(path):
            for file in os.listdir(path):
                subpath = os.path.join(path, file)
                self.upload(subpath)
        else:
            print 'wrong'

    def fetch_upload(self, files, tmp_dir):
        upload_info = []
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
            cmd = ['/home/m/mjwtom/bin/swift',
                   '-A',
                   'http://%s:8080/auth/v1.0' % proxy_ip,
                   '-U',
                   'test:tester',
                   '-K',
                   'testing',
                   'upload',
                   'mjwtom',
                   local_path]
            cmd = ['swift',
                   '-A',
                   'http://%s:8080/auth/v1.0' % proxy_ip,
                   '-U',
                   'test:tester',
                   '-K',
                   'testing',
                   'upload',
                   'mjwtom',
                   local_path]
            print 'uploading %s' % local_path
            start = time()
            ret = subprocess.call(cmd)
            end = time()
            if ret == 0:
                print 'success upload file'
            else:
                print 'fail to upload file'
            time_used = time_diff(start, end)
            size = os.path.getsize(local_path)
            throughput = size/time_used
            info = 'upload %s, size %d, time %f, throughput %f\n' % (local_path, size, time_used, throughput)
            print info
            self.info(info)
            info = dict(
                file = local_path,
                size = size,
                time = time_used,
                throughput = throughput
            )
            upload_info.append(info)
            cmd = ['rm',
                   '-rf',
                   local_path]
            subprocess.call(cmd)
        return upload_info

    def download(self, file):
        cmd =['/home/mjwtom/bin/swift'
                  '-A'
                  'http://%s:8080/auth/v1.0' % proxy_ip,
                  '-U',
                  'test:tester',
                  '-K',
                  'testing',
                  'download',
                  file]
        start = time()
        subprocess.call(cmd)
        end = time()
        time_used = time_diff(start, end)
        size = os.path.getsize(file)
        throughput = size/time_used
        info = 'upload %s, size %d, time %f, throughput %f\n' % (file, size, time_used, throughput)
        print info
        self.info(info)
        info = dict(
            file = file,
            size = size,
            time = time_used,
            throughput = throughput
        )
        cmd = ['rm',
               '-rf',
               file]
        subprocess.call(cmd)
        return info

    def sequential_download(self, files):
        download_info = []
        for file in files:
            download_info.append(self.download(file))
        return download_info

    def random_download(self, files):
        download_info = []
        l = len(files)
        while l > 0:
            index = randint(0, l-1)
            file = files.pop(index)
            download_info.append(self.download(file))
            l = len(files)
        return download_info

    def scan_dir(self, path, pickle_file):
        if not os.path.exists(path):
            print 'path does not exist'
        def deep_scan(path, files):
            if os.path.isfile(path):
                files.append(path)
            else:
                if os.path.isdir(path):
                    for file in os.listdir(path):
                        subpath = os.path.join(path, file)
                        deep_scan(subpath, files)

        files = []
        deep_scan(path, files)
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