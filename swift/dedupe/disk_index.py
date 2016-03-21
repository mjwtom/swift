from directio import read, write
import six.moves.cPickle as pickle
import os
import shutil
from hashlib import md5
from swift.common.utils import config_true_value
import sqlite3
from swift.dedupe.time import time, time_diff


class DatabaseTable(object):
    def __init__(self, conf):
        self.db_name = conf.get('data_base', ':memory:')
        if not self.db_name.endswith('.db') and not self.db_name == ':memory:':
            self.db_name += '.db'
        self.conn = sqlite3.connect(self.db_name)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS fp_index (fp text PRIMARY KEY NOT NULL, container_id text)''')
        self.fp_buf = dict()
        self.db_max_buf = int(conf.get('db_max_buf_fp', 1024))

    def __del__(self):
        self.conn.close()

    def put(self, fp, container_id):
        self.fp_buf[fp] = container_id
        if len(self.fp_buf) >= self.db_max_buf:
            for fp, container_id in self.fp_buf.items():
                data = (fp, container_id)
                self.c.execute('INSERT INTO fp_index VALUES (?, ?)', data)
            self.conn.commit()
            self.fp_buf = dict()

    def get(self, fp):
        r = self.fp_buf.get(fp, None)
        if r:
            return r
        data = (fp,)
        self.c.execute('SELECT container_id FROM fp_index WHERE fp=?', data)
        r = self.c.fetchall()
        if r:
            r = r[0][0]
        return r


class DiskHashTable(object):
    def __init__(self, conf):
        self.index_size = int(conf.get('disk_hash_table_index_size', 1024))
        self.direct_io = config_true_value(conf.get('disk_hash_table_directio', 'false'))
        self.disk_hash_dir = conf.get('disk_hash_table_dir', '/tmp/swift/disk-hash/')
        self.flush_size = int(conf.get('disk_hash_table_flush_size', 1024))
        self.memory_bucket = []
        self.bucket_lens = []
        for _ in range(self.index_size):
            self.memory_bucket.append(dict())
            self.bucket_lens.append([])
        if config_true_value(conf.get('clean_disk_hash', 'false')):
            if os.path.exists(self.disk_hash_dir):
                shutil.rmtree(self.disk_hash_dir)
        if not os.path.exists(self.disk_hash_dir):
            os.makedirs(self.disk_hash_dir)
        self.read_disk_num = 0
        self.read_disk_time = 0
        self.write_disk_num = 0
        self.write_disk_time = 0
        self.hit_num = 0

    def _map_bucket(self, key):
        h = md5(key)
        h = h.hexdigest()
        index = int(h.upper(), 16)
        index %= self.index_size
        return index

    def put(self, key, value):
        index = self._map_bucket(key)
        self.memory_bucket[index][key] = value
        if len(self.memory_bucket[index]) >= self.flush_size:
            self.flush(index)

    def flush(self, bucket_index):
        dedupe_start = time()
        if not os.path.exists(self.disk_hash_dir):
            os.makedirs(self.disk_hash_dir)
        path = self.disk_hash_dir + '/' + str(bucket_index)
        data = pickle.dumps(self.memory_bucket[bucket_index])
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
        self.bucket_lens[bucket_index].append(len(data))
        self.memory_bucket[bucket_index] = dict()
        dedupe_end = time()
        self.write_disk_num += 1
        self.write_disk_time += time_diff(dedupe_start, dedupe_end)

    def get_disk_buckets(self, index):
        dedupe_start = time()
        buckets = []
        path = self.disk_hash_dir + '/' + str(index)
        if not os.path.exists(path):
            return buckets
        file_size = os.path.getsize(path)
        data = ''
        if self.direct_io:
            f = os.open(path, os.O_RDONLY | os.O_DIRECT)
            try:
                data = read(f, file_size)
            except Exception as e:
                print e
            finally:
                os.close(f)
        else:
            with open(path, 'rb') as f:
                data = f.read()
        if not data:
            print 'read data failed'
        offset = 0
        for l in self.bucket_lens[index]:
            bucket_data = data[offset:offset+l]
            bucket = pickle.loads(bucket_data)
            buckets.append(bucket)
            offset += l
        dedupe_end = time()
        self.read_disk_num += 1
        self.read_disk_time += time_diff(dedupe_start, dedupe_end)
        return buckets

    def get(self, key):
        index = self._map_bucket(key)
        r = self.memory_bucket[index].get(key, None)
        if r:
            self.hit_num += 1
            return r
        path = self.disk_hash_dir + '/' + str(index)
        if not os.path.exists(path):
            return None
        buckets = self.get_disk_buckets(index)
        for bucket in buckets:
            r = bucket.get(key)
            if r:
                self.hit_num += 1
                return r
        return None


class LazyHashTable(DiskHashTable):
    def __init__(self, conf, callback= None):
        DiskHashTable.__init__(self, conf)
        self.lazy_bucket_size = int(conf.get('lazy_bucket_size', 32))
        self.lazy_bucket = []
        self.callback = callback
        self.buffer = set()
        for _ in range(self.index_size):
            self.lazy_bucket.append(dict())

    def _lookup_in_bucket(self, index, bucket):
        result = []
        for fp, v in self.lazy_bucket[index].items():
            r = bucket.get(fp, None)
            if r:
                self.hit_num += 1
                result.append((fp, r, v))
                del self.lazy_bucket[index][fp]
        return result

    def lazy_lookup(self, index):
        result = []
        if self.lazy_bucket[index]:
            result += self._lookup_in_bucket(index, self.memory_bucket[index])
        path = self.disk_hash_dir + '/' + str(index)
        if os.path.exists(path):
            buckets = self.get_disk_buckets(index)
            for bucket in buckets:
                result.extend(self._lookup_in_bucket(index, bucket))
        # the unfound fingerprints are unique
        for fp, v in self.lazy_bucket[index].items():
            result.append((fp, None, v))
            del self.lazy_bucket[index][fp]
        return result

    def buf(self, fp, value):
        index = self._map_bucket(fp)
        if fp not in self.lazy_bucket[index]:
            self.lazy_bucket[index][fp] = [value]
        else:
            if value not in self.lazy_bucket[index][fp]:
                self.lazy_bucket[index][fp].append(value)
        if len(self.lazy_bucket[index]) >= self.lazy_bucket_size:
            result = self.lazy_lookup(index)
            self.callback(result)

    def buf_remove(self, fp):
        index = self._map_bucket(fp)
        if fp in self.lazy_bucket[index]:
            del self.lazy_bucket[index][fp]

    def buf_get(self, fp):
        index = self._map_bucket(fp)
        return self.lazy_bucket[index].get(fp)