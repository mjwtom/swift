from directio import read, write
import six.moves.cPickle as pickle
import os
import shutil
from hashlib import md5
from swift.common.utils import config_true_value
import sqlite3


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
        self.flus_size = int(conf.get('disk_hash_table_flush_size', 1024))
        self.memory_bucket = []
        self.bucket_lens = []
        for _ in range(self.index_size):
            self.memory_bucket.append(dict())
            self.bucket_lens.append([])
        if config_true_value(conf.get('clean_disk_hash', 'false')):
            if os.path.exists(self.disk_hash_dir):
                shutil.rmtree(self.disk_hash_dir)

    def _map_bucket(self, key):
        h = md5(key)
        h = h.hexdigest()
        k = int(h.upper(), 16)
        k %= self.index_size
        return k

    def put(self, key, value):
        k = self._map_bucket(key)
        self.memory_bucket[k][key] = value
        if len(self.memory_bucket[k]) >= self.flus_size:
            self.flush(k)

    def flush(self, bucket_index):
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

    def get(self, key):
        k = self._map_bucket(key)
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


class LazyHashTable(DiskHashTable):
    def __init__(self, conf, callback= None):
        DiskHashTable.__init__(self, conf)
        self.lazy_bucket_size = int(conf.get('lazy_bucket_size', 16))
        self.lazy_bucket = []
        self.callback = callback
        self.buffer = set()
        for _ in range(self.index_size):
            self.lazy_bucket.append(dict())

    def _lookup_in_bucket(self, k, bucket):
        result = []
        for fp, v in self.lazy_bucket[k].items():
            r = bucket.get(fp, None)
            if r:
                result.append((fp, r, v))
                del self.lazy_bucket[k][fp]
        return result

    def lazy_lookup(self, k):
        result = []
        if self.lazy_bucket[k]:
            result += self._lookup_in_bucket(k, self.memory_bucket[k])
        path = self.disk_hash_dir + '/' + str(k)
        if os.path.exists(path):
            if self.direct_io:
                f = os.open(path, os.O_RDONLY | os.O_DIRECT)
                try:
                    for ll in self.bucket_lens[k]:
                        if not self.lazy_bucket[k]:
                            break
                        data = read(f, ll)
                        data = pickle.loads(data)
                        result += self._lookup_in_bucket(k, data)
                except Exception as e:
                    pass
                finally:
                    os.close(f)
            else:
                with open(path, 'rb') as f:
                    for ll in self.bucket_lens[k]:
                        if not self.lazy_bucket[k]:
                            break
                        data = f.read(ll)
                        data = pickle.loads(data)
                        result += self._lookup_in_bucket(k, data)
        for fp, v in self.lazy_bucket[k].items():
            result.append((fp, None, v))
            del self.lazy_bucket[k][fp]
        return result

    def buf(self, fp, value):
        k = self._map_bucket(fp)
        if fp not in self.lazy_bucket[k]:
            self.lazy_bucket[k][fp] = [value]
        else:
            if value not in self.lazy_bucket[k][fp]:
                self.lazy_bucket[k][fp].append(value)
        if len(self.lazy_bucket[k]) >= self.lazy_bucket_size:
            result = self.lazy_lookup(k)
            self.callback(result)

    def buf_remove(self, fp):
        k = self._map_bucket(fp)
        if fp in self.lazy_bucket[k]:
            del self.lazy_bucket[k][fp]

    def buf_get(self, fp):
        k = self._map_bucket(fp)
        return self.lazy_bucket[k].get(fp)