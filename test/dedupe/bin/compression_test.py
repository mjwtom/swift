#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-

import zlib
import lz4
import sys
from datetime import datetime


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


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'please give the test file path'
        exit()
    compare(sys.argv[1])
