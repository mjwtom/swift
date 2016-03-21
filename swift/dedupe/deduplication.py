from pybloom import ScalableBloomFilter
from swift.dedupe.dedupe_container import DedupeContainer
from swift.dedupe.summary import DedupeSummary
from swift.common.utils import config_true_value, get_logger
from swift.common.swob import Request
from hashlib import md5
import sqlite3
from disk_index import DatabaseTable, DiskHashTable, LazyHashTable
from swift.common.wsgi import make_env
from swift.common.swob import WsgiBytesIO
from lru import LRU
import os
import six.moves.cPickle as pickle
from directio import read, write
from copy import copy
from swift.dedupe.compress import compress, decompress
import shutil
from swift.dedupe.time import time, time_diff
# TODO: change this back to greenpool, but now, I have no time to do this, so I use threading
#from eventlet.queue import Queue
#from swift.common.utils import ContextPool
from threading import Thread
import Queue


class ChunkStore(object):
    def __init__(self, conf, app, logger=None):
        self.app = app
        self.object_controller = None
        self.req = None
        self.mylogpath = conf.get('mylog', '/tmp/deduplication.txt')
        self.direct_io = config_true_value(conf.get('load_fp_directio', 'false'))
        self.chunk_store_version = conf.get('chunk_store_version', 'v1')
        self.chunk_store_account = conf.get('chunk_store_account', 'chunk_store')
        self.chunk_sore_container = conf.get('chunk_sore_container', 'chunk_store')
        self.fp_cache = LRU(int(conf.get('cache_size', 1024*1024*32)))
        self.chunk_pool = LRU(int(conf.get('chunk_pool_size', 1024*256)))
        self.container_pool = LRU(int(conf.get('compress_pool_size', 256)))
        self.bf = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        self.dc_size = int(conf.get('dedupe_container_size', 4096))
        self.summary = DedupeSummary()
        self.next_dc_id = int(conf.get('next_dc_id', 0))
        self.container = None
        self.new_container()
        self.container_fp_dir = conf.get('dedupe_container_fp_dir', '/tmp/swift/container_fp/')
        if config_true_value(conf.get('clean_container_fp', 'false')):
            if os.path.exists(self.container_fp_dir):
                shutil.rmtree(self.container_fp_dir)
        if not os.path.exists(self.container_fp_dir):
            os.makedirs(self.container_fp_dir)
        self.sqlite_index = config_true_value(conf.get('sqlite_index', 'false'))
        if self.sqlite_index:
            self.index = DatabaseTable(conf)
        else:
            self.lazy_dedupe = config_true_value(conf.get('lazy_dedupe', 'true'))
            if self.lazy_dedupe:
                self.index = LazyHashTable(conf, self.lazy_callback)
                self.lazy_max_unique = int(conf.get('lazy_max_unique', 200))
                self.lazy_max_fetch = int(conf.get('lazy_max_fetch', self.dc_size))
                self.dup_cluster = dict()
                self.current_continue_dup = 0
            else:
                self.index = DiskHashTable(conf)
        self.compress = config_true_value(conf.get('compress', 'false'))
        if self.compress:
            self.method = conf.get('compress_method', 'lz4hc')
        self.async_send = config_true_value(conf.get('async_send', 'true'))
        if self.async_send:
            self.send_queue_len = int(conf.get('send_queue_len', 2))
            self.send_queue = Queue.Queue(self.send_queue_len)
            self.sender = Thread(target=self.send_container_thread, args=(self.send_queue,))#self.send_pool = ContextPool(1) # only one send thread is enough
            self.sender.start()
            #self.send_pool.spawn(self.send_container_thread, self.send_queue)
        if logger is None:
            log_conf = dict()
            for key in ('log_facility', 'log_name', 'log_level', 'log_udp_host',
                    'log_udp_port', 'log_statsd_host', 'log_statsd_port',
                    'log_statsd_default_sample_rate',
                    'log_statsd_sample_rate_factor',
                    'log_statsd_metric_prefix'):
                value = conf.get('dedupe_' + key, conf.get(key, None))
                if value:
                    log_conf[key] = value
            self.logger = get_logger(log_conf, log_route='deduplication')
        else:
            self.logger = logger
        self.container_penalty = config_true_value(conf.get('dedupe_container_penalty', 'false'))

    def _add2cache(self, cache, key, value):
        cache[key] = value

    def _get_from_cache(self, cache, key):
        return cache.get(key)

    def hash(self, data):
        return md5(data).hexdigest()

    def log_message(self, messages):
        dir, filename = os.path.split(self.mylogpath)
        if not os.path.exists(dir):
            os.makedirs(dir)
        f = open(self.mylogpath, 'a')
        for m in messages:
            f.write(m+'\n')
        f.write('\n')
        f.flush()
        f.close()

    def new_container(self):
        self.container = DedupeContainer(str(self.next_dc_id), self.dc_size)
        self.next_dc_id += 1

    def _store_container_fp(self, container):
        if not os.path.exists(self.container_fp_dir):
            os.makedirs(self.container_fp_dir)
        path = self.container_fp_dir + '/' + container.get_id()
        fps = container.get_fps()
        data = pickle.dumps(fps)
        if self.direct_io:
            f = os.open(path, os.O_CREAT | os.O_APPEND | os.O_RDWR | os.O_DIRECT)
            len_data = len(data)
            if (len_data % 512) != 0:
                ll = 512 - len_data%512 # alligned by 512
                data += '\0'*ll
            try:
                write(f, data)
            except Exception as e:
                pass
            finally:
                os.close(f)
        else:
            with open(path, 'wb') as f:
                pickle.dump(fps, f)

    def _load_container_fp(self, container_id):
        if container_id == self.container.get_id():
            fps = self.container.get_fps()
            return fps
        path = self.container_fp_dir + '/' + container_id
        if not os.path.exists(path):
            return None
        if self.direct_io:
            ll = os.path.getsize(path)
            f = os.open(path, os.O_RDONLY | os.O_DIRECT)
            try:
                data = read(f, ll)
                fps = pickle.loads(data)
            except Exception as e:
                pass
            finally:
                os.close(f)
        else:
            with open(path, 'rb') as f:
                fps = pickle.load(f)
        return fps

    def _fill_cache_with_container_fp(self, container_id):
        fps = self._load_container_fp(container_id)
        for fp in fps:
            self._add2cache(self.fp_cache, fp, container_id)

    #FIXME: this is tmperal method to duplicate a req and change something
    #This should be changed to create a request all by the chunk store
    def _dupl_req(self, req, obj_name, data_source, data_len, compressed=False, method='lz4hc'):
        """
        Makes a copy of the request, converting it to a GET.
        """

        env = req.environ.copy()
        l = len(self.object_controller.object_name)
        tmp_pth = env['PATH_INFO'][:-l] + str(obj_name)
        env.update({
                'CONTENT_LENGTH': str(data_len),
                'wsgi.input': data_source,
                'PATH_INFO': tmp_pth
                })
        req = Request(env)
        if compressed:
            req.headers.update({
                'X-Object-Sysmeta-Compressed': str(compressed),
                'X-Object-Sysmeta-CompressionMethod': method
            })
        return req

    def send_container_thread(self, queue):
        while True:
            container = queue.get()
            self._store_container(container)
        queue.taskdone()

    def _store_container(self, container):
        dedupe_start = time()
        data = container.dumps()
        dedupe_end = time()
        self.summary.container_pickle_dumps_time += time_diff(dedupe_start, dedupe_end)
        path = '/' + self.chunk_store_version
        path += '/' + self.chunk_store_account
        path += '/' + self.chunk_sore_container
        path += '/' + container.get_id()
        environ = dict(
            CONTENT_LENGTH = str(len(data))
        )
        if self.compress:
            dedupe_start = time()
            data = compress(data, self.method)
            dedupe_end = time()
            self.summary.compression_time += time_diff(dedupe_start, dedupe_end)
        ll = len(data)
        data = WsgiBytesIO(data)
        environ = make_env(environ, method='PUT', path = path)
        environ['wsgi.input'] = data
        req = Request(environ)
        '''
        FIXME: Do not use the oject controller borrowed  from the data source,
        Use your own please
        '''
        if self.compress:
            req = self._dupl_req(self.req, container.get_id(), data, ll, self.compress, self.method)
        else:
            req = self._dupl_req(self.req, container.get_id(), data, ll)
        try:
            controller = copy(self.object_controller)
            #obj_name = self.object_controller.object_name
            controller.object_name = container.get_id()
            resp = controller.store_container(req)
            #self.object_controller.object_name = obj_name
        except Exception as e:
            pass
        del resp

    def store(self, fp, chunk):
        container_id = self.container.get_id()
        self.container.add(fp, chunk)
        if self.container.is_full():
            full_container = self.container
            self.new_container()
            self._store_container_fp(full_container)
            dedupe_start = self.summary.time()
            if self.async_send:
                self.send_queue.put(full_container)
            else:
                self._store_container(full_container)
            dedupe_end = self.summary.time()
            self.summary.store_time += self.summary.time_diff(dedupe_start, dedupe_end)
        return container_id

    def _lazy_dedupe(self, fp, chunk):
        dedupe_start = self.summary.time()
        if not fp in self.bf:
            self.bf.add(fp)
            container_id = self.store(fp, chunk)
            self.index.put(fp, container_id)
            return

        self.current_continue_dup += 1
        if self._get_from_cache(self.fp_cache, fp):
            self.summary.dupe_size += len(chunk) # for summary
            self.summary.dupe_chunk += 1 # for summary
            dedupe_end = self.summary.time()
            time_diff = self.summary.time_diff(dedupe_start, dedupe_end)
            self.summary.fp_lookup_time += time_diff
            return
        if self.current_continue_dup >= self.lazy_max_fetch:
            self.dup_cluster = dict()
            self.current_continue_dup = 0
        self.dup_cluster[fp] = chunk
        self.index.buf(fp, self.dup_cluster)
        dedupe_end = self.summary.time()
        time_diff = self.summary.time_diff(dedupe_start, dedupe_end)
        self.summary.fp_lookup_time += time_diff

    def lazy_callback(self, result):
        load_container_set = set()
        for fp, container_id, clusters in result:
            # if a fingperint is duplicate, container_id not none
            # only if it is also has cluster, the system loads fingerprints
            if container_id and clusters:
                if container_id not in load_container_set:
                    self._fill_cache_with_container_fp(container_id)
                    load_container_set.add(container_id)
                for cluster in clusters:
                    for fp, chunk in cluster.items():
                        if self._get_from_cache(self.fp_cache, fp):
                            self.summary.write_cache_hit += 1 #for summary
                            self.summary.dupe_size += len(chunk) # for summary
                            self.summary.dupe_chunk += 1 # for summary
                            self.index.buf_remove(fp)
                            del cluster[fp]
            else:
                # The fingerprint is unique, we need to store the chunk
                # The for loop is used to find the chunk, once we found, break
                for cluster in clusters:
                    chunk = cluster.get(fp) # find the chunk
                    if chunk:
                        break
                container_id = self.store(fp, chunk)
                self.index.put(fp, container_id)
                for cluster in clusters:
                    del cluster[fp]

    def _eager_dedupe(self, fp, chunk):
        dedupe_start = self.summary.time()
        if fp in self.bf:
            if self._get_from_cache(self.fp_cache, fp):
                self.summary.write_cache_hit += 1 # for summary
                self.summary.dupe_size += len(chunk) # for summary
                self.summary.dupe_chunk += 1 # for summary
                dedupe_end = self.summary.time()
                time_diff = self.summary.time_diff(dedupe_start, dedupe_end)
                self.summary.fp_lookup_time += time_diff
                return
            container_id = self.index.get(fp)
            if container_id:
                self.summary.dupe_size += len(chunk) # for summary
                self.summary.dupe_chunk += 1 # for summary
                dedupe_end = self.summary.time()
                time_diff = self.summary.time_diff(dedupe_start, dedupe_end)
                self.summary.fp_lookup_time += time_diff
                self._fill_cache_with_container_fp(container_id)
                return
        self.bf.add(fp)
        container_id = self.store(fp, chunk)
        self.index.put(fp, container_id)

    def put(self, fp, chunk, obj_controller, req):
        self.summary.total_size += len(chunk) # for summary
        self.summary.total_chunk += 1 # for summary
        self.object_controller = obj_controller
        self.req = req
        if self.sqlite_index:
            return self._sqlite_get(fp)
        elif self.lazy_dedupe:
            self._lazy_dedupe(fp, chunk)
        else:
            self._eager_dedupe(fp, chunk)

    def _sqlite_get(self, fp):
        return self._eager_get(fp)

    def add_to_chunk_pool(self, container):
        for fp, chunk in container.kv.items():
            self.chunk_pool[fp] = chunk

    def _eager_get(self, fp):
        r = self.container.get(fp)
        if r:
            return r
        r = self.chunk_pool.get(fp)
        if r:
            self.summary.hit_uncompressed += 1
            return r
        dedupe_start = time()
        dc_id = self.index.get(fp)
        dedupe_end = time()
        self.summary.get_cid_time += time_diff(dedupe_start, dedupe_end)

        cdc = self.container_pool.get(dc_id)
        if cdc:
            self.summary.hit_compressed += 1
            dedupe_start = time()
            data = decompress(cdc, self.method)
            dedupe_end = time()
            self.summary.decompression_time += time_diff(dedupe_start, dedupe_end)
            dc_container = DedupeContainer(dc_id)
            dedupe_start = time()
            dc_container.loads(data)
            dedupe_end = time()
            self.summary.container_pickle_loads_time += time_diff(dedupe_start, dedupe_end)
            self.add_to_chunk_pool(dc_container)
            r = self.chunk_pool.get(fp)
            if r:
                return r
            else:
                return dc_container.get(fp)


        req = self._dupl_req(self.req, dc_id, None, 0)

        obj_name = self.object_controller.object_name
        self.object_controller.object_name = dc_id
        resp = self.object_controller.GETorHEAD(req)
        self.object_controller.object_name = obj_name

        data = ''
        for d in iter(resp.app_iter):
            data += d
        if resp.headers.get('X-Object-Sysmeta-Compressed'):
            self.container_pool[dc_id] = data
            self.method = resp.headers.get('X-Object-Sysmeta-CompressionMethod')
            dedupe_start = self.summary.time()
            data = decompress(data, self.method)
            dedupe_end = self.summary.time()
            self.summary.decompression_time += self.summary.time_diff(dedupe_start, dedupe_end)
        del resp
        dc_container = DedupeContainer(dc_id)
        dedupe_start = time()
        dc_container.loads(data)
        dedupe_end = time()
        self.summary.container_pickle_loads_time += time_diff(dedupe_start, dedupe_end)
        self.add_to_chunk_pool(dc_container)
        r = self.chunk_pool.get(fp)
        if r:
            return r
        else:
            return dc_container.get(fp)

    def _lazy_get(self, fp):
        #check if the chunk in the buffer area
        clusters = self.index.buf_get(fp)
        if clusters:
            for cluster in clusters:
                if fp in cluster:
                    return cluster.get(fp)
        return self._eager_get(fp)

    def get(self, fp, controller, req):
        self.object_controller = controller
        self.req = req
        self.summary.get += 1
        if self.sqlite_index:
            return self._sqlite_get(fp)
        elif self.lazy_dedupe:
            return self._lazy_get(fp)
        else:
            return self._eager_get(fp)


class InformationDatabase(object):
    def __init__(self, conf):
        self.db_name = conf.get('data_base', ':memory:')
        if not self.db_name.endswith('.db') and not self.db_name == ':memory:':
            self.db_name += '.db'
        self.conn = sqlite3.connect(self.db_name)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS obj_fps (obj text PRIMARY KEY NOT NULL, fps text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS obj_etag (obj text PRIMARY KEY NOT NULL, etag text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS dc_rc(dc text PRIMARY KEY NOT NULL, rc text)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS dc_device(id int auto_increment primary key not null,
                        dc text, dev text)''')

    def __del__(self):
        self.conn.close()

    def insert_obj_fps(self, obj_hash, fps):
        data = (obj_hash, fps)
        self.c.execute('INSERT INTO obj_fps VALUES (?, ?)', data)
        self.conn.commit()

    def lookup_obj_fps(self, obj_hash):
        data = (obj_hash,)
        self.c.execute('SELECT value FROM fp_index WHERE obj=?', data)
        r = self.c.fetchall()
        if r:
            r = r[0][0]
        return r

    def insert_etag(self, key, value):
        data = (key, value)
        self.c.execute('INSERT INTO obj_etag VALUES (?, ?)', data)
        self.conn.commit()

    def lookup_etag(self, key):
        data = (key,)
        self.c.execute('SELECT etag FROM obj_etag WHERE obj=?', data)
        r = self.c.fetchall()
        if r:
            r = r[0][0]
        return r

    def insert_rc(self, dc, rc):
        data = (dc, rc)
        self.c.execute('UPDATE dc_rc VALUES(rc=?) WHERE dc=?', data)
        self.conn.commit()

    def get_rc(self, dc):
        data = (dc,)
        self.c.execute('SELECT rc from dc_rc where dc=?', data)
        r = self.c.fetchall()
        if r:
            r = r[0][0]
        return r

    def update_rc(self, dc, rc):
        data = (rc, dc)
        self.c.execute('update dc_rc set rc=? WHERE dc=?', data)
        self.conn.commit()

    def get_all_rc(self):
        self.c.execute('SELECT * FROM dc_container_rc')
        kall = self.c.fetchall()
        return kall

    def get_dev_dc(self, dev):
        data = (dev,)
        self.c.execute('SELECT dc FROM dc_device where dev=?', data)
        kall = self.c.fetchall()
        return kall

    def batch_update_rc(self, dc_rc):
        for (dc, rc) in dc_rc.items():
            rc = str(rc)
            data = (dc,)
            self.c.execute('SELECT dc FROM dc_device where dev=?', data)
            r = self.c.fetchall()
            if r:
                data = (rc, dc)
                self.c.execute('update dc_rc set rc=? where id=?', data)
            else:
                data = (dc, rc)
                self.c.execute('insert into dc_rc values (?, ?)', data)
        self.conn.commit()