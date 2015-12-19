from directio import read, write
import six.moves.cPickle as pickle
import os
import shutil
from hashlib import md5
from swift.common.utils import config_true_value


class DiskHashtable(object):
    def __init__(self, conf):
        self.index_len = int(conf.get('disk_hash_table_index_len', 1024))
        self.direct_io = config_true_value(conf.get('disk_hash_table_directio', 'false'))
        self.disk_hash_dir = conf.get('disk_hash_table_dir', '/tmp/swift-disk-hash/')
        self.flus_num = int(conf.get('disk_hash_table_flush_num', 1024))
        self.memory_bucket = []
        self.bucket_lens = []
        for _ in range(self.index_len):
            self.memory_bucket.append(dict())
            self.bucket_lens.append([])
        if config_true_value(conf.get('clean_disk_hash', 'false')):
            if os.path.exists(self.disk_hash_dir):
                shutil.rmtree(self.disk_hash_dir)

    def add_kv(self, key, value):
        h = md5(key)
        h = h.hexdigest()
        k = int(h.upper(), 16)
        k %= self.index_len
        self.memory_bucket[k][key] = value
        if len(self.memory_bucket[k]) >= self.flus_num:
            self.flush(k)

    def flush(self, bucket_num):
        if not os.path.exists(self.disk_hash_dir):
            os.makedirs(self.disk_hash_dir)
        path = self.disk_hash_dir + '/' + str(bucket_num)
        data = pickle.dumps(self.memory_bucket[bucket_num])
        with open(path, 'ab') as f:
            if self.direct_io:
                f.write(data)
            else:
                f.write(data)
        self.bucket_lens[bucket_num].append(len(data))
        self.memory_bucket[bucket_num] = dict()

    def lookup(self, key):
        h = md5(key)
        h = h.hexdigest()
        k = int(h.upper(), 16)
        k %= self.index_len
        r = self.memory_bucket[k].get(key, None)
        if r:
            return r
        path = self.disk_hash_dir + '/' + str(k)
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            for len in self.bucket_lens[k]:
                if self.direct_io:
                    data = f.read(len)
                else:
                    data = f.read(len)
                data = pickle.loads(data)
                r = data.get(key, None)
                if r:
                    return r
        return None