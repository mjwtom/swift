#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-

import zlib
import lz4
import sys
from datetime import datetime
import os
import pickle
from hashlib import md5
from nodes import storage_nodes as ips, usr, port, pwd
from threading import Thread
from test.dedupe.ssh import run_cmd, download


def compare(path, blksize=16*1024*1024):
    for compress, decompress in [(zlib.compress, zlib.decompress), (lz4.compressHC, lz4.decompress)]:
        size = 0
        compress_size = 0
        time_sum = 0
        time_sum_dec = 0
        with open(path) as f:
            data = f.read(blksize)
            while data:
                size += len(data)
                start_time = datetime.now()
                data = compress(data)
                end_time = datetime.now()
                compress_size += len(data)
                diff = end_time - start_time
                time_sum += diff.total_seconds()
                start_time = datetime.now()
                decompress(data)
                end_time = datetime.now()
                diff = end_time - start_time
                time_sum_dec += diff.total_seconds()
                data = f.read(blksize)
        if compress == zlib.compress:
            method = 'zlib'
        elif compress == lz4.compressHC:
            method = 'lz4hc'
        else:
            method = 'unkonwn compression'
        print '%s' % method
        print 'total data size %d' % size
        print 'compressed size %d' % compress_size
        print 'compression ratio %f' % (compress_size*1.0/size)
        print 'compression time %f seconds' % time_sum
        print 'compression speed %f MB/second' % (size/time_sum/1024/1024)
        print 'decompression time %f seconds' % time_sum_dec
        print 'decompression speed %f MB/second' % (size/time_sum_dec/1024/1024)
        print '\n\n'


def compress_files(path, dst_file):
    if not os.path.exists(path):
        print 'there is not that path %s' % path
        return

    def recur_compress(path, result):
        if os.path.isfile(path):
            print 'compression file %s\n' % path
            with open(path, 'rb') as f:
                data = f.read()
                hash = md5(data)
                hash = hash.hexdigest()
                orig_size = len(data)
                data = lz4.compressHC(data)
                compressed_size = len(data)
            dir, name = os.path.split(path)
            info = dict(
                md5 = hash,
                name = name,
                orig_size = orig_size,
                compressed_size = compressed_size
            )
            result.append(info)
            print '%s original size %d, compressed size %d' % (name, orig_size, compressed_size)
        else:
            for file in os.listdir(path):
                subpath = os.path.join(path, file)
                recur_compress(subpath, result)

    result = []
    recur_compress(path, result)
    f = open(dst_file, 'wb')
    pickle.dump(result, f)
    f.close()


def getsize_files(path, dst_file):
    if not os.path.exists(path):
        print 'there is not that path %s' % path
        return

    def recur_getsize(path, result):
        if os.path.isfile(path):
            size = os.path.getsize(path)
            dir, name = os.path.split(path)
            info = dict(
                md5 = hash,
                name = name,
                orig_size = size,
                compressed_size = size
            )
            result.append(info)
            print '%s original size %d, compressed size %d' % (name, size, size)
        else:
            for file in os.listdir(path):
                subpath = os.path.join(path, file)
                recur_getsize(subpath, result)

    result = []
    recur_getsize(path, result)
    f = open(dst_file, 'wb')
    pickle.dump(result, f)
    f.close()


def thread_node_compress():
    cmd = '/home/m/mjwtom/bin/python ' \
          '/home/m/mjwtom/swift/test/dedupe/bin/compression_test.py ' \
          '/home/m/mjwtom/swift-data/sdb1/objects /home/m/mjwtom/compression.pickle'
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, cmd)
        threads.append(Thread(target=run_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def thread_node_getsize():
    cmd = '/home/m/mjwtom/bin/python ' \
          '/home/m/mjwtom/swift/test/dedupe/bin/compression_test.py size ' \
          '/home/m/mjwtom/swift-data/sdb1/objects-2 /home/m/mjwtom/size.pickle'
    threads = []
    for ip in ips:
        args = (usr, ip, port, pwd, cmd)
        threads.append(Thread(target=run_cmd, args=args))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def collect_compress():
    remote = '/home/m/mjwtom/compression.pickle'
    dir = '/home/mjwtom/compression-kernel'
    if not os.path.exists(dir):
        os.makedirs(dir)
    for ip in ips:
        local = dir + ('/%s.pickle' % ip)
        download(usr, ip, port, pwd, local, remote)


def collect_size():
    remote = '/home/m/mjwtom/size.pickle'
    dir = '/home/mjwtom/size-kernel'
    if not os.path.exists(dir):
        os.makedirs(dir)
    for ip in ips:
        local = dir + ('/%s.pickle' % ip)
        download(usr, ip, port, pwd, local, remote)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'please give the test file path'
        exit()
    if len(sys.argv) == 2:
        if sys.argv[1] == 'basic':
            compare(sys.argv[1])
        elif sys.argv[1] == 'storage_compress':
            thread_node_compress()
        elif sys.argv[1] == 'storage_size':
            thread_node_getsize()
        elif sys.argv[1] == 'collect_compress':
            collect_compress()
        elif sys.argv[1] == 'collect_size':
            collect_size()
    else:
        directory = sys.argv[2]
        pickle_file = sys.argv[3]
        if sys.argv[1] == 'compress':
            compress_files(directory, pickle_file)
        elif sys.argv[1] == 'size':
            getsize_files(directory, pickle_file)
