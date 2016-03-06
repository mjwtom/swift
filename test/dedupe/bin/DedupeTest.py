#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-
import os
from swift.dedupe.summary import DedupeSummary
import subprocess
from nodes import proxy_ip
from test.dedupe.ssh import SSH
from nodes import file_server, file_dir


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
            dedupe_start = DedupeSummary.time()
            subprocess.call(cmd)
            dedupe_end = DedupeSummary.time()
            time = DedupeSummary.time_diff(dedupe_start, dedupe_end)
            size = os.path.getsize(file)
            throughput = size/time
            info = dict(
                file = path,
                size = size,
                time = time,
                throughput = throughput
            )
            self.uploads.append(info)
        elif os.path.isdir(path):
            for file in os.listdir(path):
                subpath = os.path.join(path, file)
                self.upload(subpath)
        else:
            print 'wrong'

    def fetch_upload(self, src, dst):
        client = SSH()

