import sys
import os
import pickle
from swift.dedupe.time import time, time_diff
import subprocess

proxy_ip = '127.0.0.1'


def upload(path):
    cmd = ['swift',
           '-A',
           'http://%s:8080/auth/v1.0' % proxy_ip,
           '-U',
           'test:tester',
           '-K',
           'testing',
           'upload',
           'mjwtom',
           path]
    print 'uploading %s' % path
    start = time()
    ret = subprocess.call(cmd)
    end = time()
    if ret == 0:
        print 'success upload file'
    else:
        print 'fail to upload file'
    time_used = time_diff(start, end)
    size = os.path.getsize(path)
    throughput = size/time_used
    print 'upload %s, size %d, time %f, throughput %f\n' % (path, size, time_used, throughput)
    info = dict(
        file = path,
        size = size,
        time = time_used,
        throughput = throughput
    )
    return info


def test_upload(data_path, result_path):
    if not os.path.exists(data_path):
        print 'no such directory'
        return
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    result = []
    for f in os.listdir(data_path):
        _, fname = os.path.split(f)
        path = os.path.join(data_path, fname)
        result.append(upload(path))
    uploaded_pickle = os.path.join(result_path, 'upload_result.pickle')
    out = open(uploaded_pickle, 'wb')
    pickle.dump(result, out)
    out.close()


if __name__=='__main__':
    if len(sys.argv) < 2:
        print 'pleas give the case to test'
        exit()
    data_path = sys.argv[1]
    result_path = sys.argv[2]
    test_upload(data_path, result_path)