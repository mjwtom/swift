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
        if self.direct_io:
            f = os.open(path, os.O_CREAT | os.O_APPEND | os.O_RDWR | os.O_DIRECT)
            ll = 512 - len(data)%512 # alligned by 512
            data += '\0'*ll
            try:
                write(f, data)
            except Exception as e:
                pass
            finally:
                os.close(f)
        else:
            with open(path, 'ab') as f:
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
        if self.direct_io:
            f = os.open(path, os.O_RDONLY | os.O_DIRECT)
            try:
                for ll in self.bucket_lens[k]:
                    data = read(f, ll)
                    data = pickle.loads(data)
                    r = data.get(key, None)
                    if r:
                        return r
            except Exception as e:
                pass
            finally:
                os.close(f)
        else:
            with open(path, 'rb') as f:
                for ll in self.bucket_lens[k]:
                    data = f.read(ll)
                    data = pickle.loads(data)
                    r = data.get(key, None)
                    if r:
                        return r
        return None