from swift.dedupe.pybloom.pybloom import BloomFilter
from swift.dedupe.dedupe_container import DedupeContainer
from swift.dedupe.state import DedupeState
from swift.common.utils import config_true_value
from swift.common.swob import Request
from hashlib import md5
import sqlite3
from disk_index import DatabaseTable, DiskHashTable, LazyHashTable
from swift.common.wsgi import make_env
from swift.common.swob import WsgiBytesIO
from lru import LRU
import lz4
import os
import six.moves.cPickle as pickle
from directio import read, write


class ChunkStore(object):
    def __init__(self, conf, app):
        self.app = app
        self.object_controller = None
        self.req = None
        self.direct_io = config_true_value(conf.get('load_fp_directio', 'false'))
        self.chunk_store_version = conf.get('chunk_store_version', 'v1')
        self.chunk_store_account = conf.get('chunk_store_account', 'chunk_store')
        self.chunk_sore_container = conf.get('chunk_sore_container', 'chunk_store')
        self.fp_cache = LRU(int(conf.get('cache_size', 65536)))
        self.dc_cache = LRU(int(conf.get('dc_cache', 4)))
        self.bf = BloomFilter(int(conf.get('bf_capacity', 1024*1024)))
        self.dc_size = int(conf.get('dedupe_container_size', 4096))
        self.state = DedupeState()
        self.next_dc_id = int(conf.get('next_dc_id', 0))
        self.new_container()
        self.container_fp_dir = conf.get('dedupe_container_fp_dir', '/tmp/swift/container_fp/')
        self.sqlite_index = config_true_value(conf.get('sqlite_index', 'false'))
        if self.sqlite_index:
            self.index = DatabaseTable(conf)
        else:
            self.lazy_dedupe = config_true_value(conf.get('lazy_dedupe', 'true'))
            if self.lazy_dedupe:
                self.index = LazyHashTable(conf, self.lazy_callback)
                self.lazy_max_unique = int(conf.get('lazy_max_unique', 200))
                self.lazy_max_fetch = int(conf.get('lazy_max_fetch', self.dc_size))
                self.dup_cluster_set = []
                self.dup_cluster = dict()
                self.dup_cluster_set.append(self.dup_cluster)
                self.current_continue_dup = 0
            else:
                self.index = DiskHashTable(conf)

    def _add2cache(self, cache, key, value):
        cache[key] = value

    def _get_from_cache(self, cache, key):
        return cache.get(key)

    def hash(self, data):
        return md5(data).hexdigest()

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
            ll = 512 - len(data)%512 # alligned by 512
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
        path = self.container_fp_dir + '/' + container_id
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
    def _dupl_req(self, req, obj_name, data_source, data_len):
        """
        Makes a copy of the request, converting it to a GET.
        """

        env = req.environ.copy()
        l = len(self.object_controller.object_name)
        tmp_pth = env['PATH_INFO'][:-l] + str(obj_name)
        env.update({
            'CONTENT_LENGTH': str(data_len),
            'wsgi.input': data_source,
            'PATH_INFO' : tmp_pth,
        })
        return Request(env)

    def _store_container(self, container):
        data = container.dumps()
        path = '/' + self.chunk_store_version
        path += '/' + self.chunk_store_account
        path += '/' + self.chunk_sore_container
        path += '/' + container.get_id()
        environ = dict(
            CONTENT_LENGTH = str(len(data))
        )
        ll = len(data)
        data = WsgiBytesIO(data)
        environ = make_env(environ, method='PUT', path = path)
        environ['wsgi.input'] = data
        req = Request(environ)
        '''
        FIXME: Do not use the oject controller borrowed  from the data source,
        Use your own please
        '''
        req = self._dupl_req(self.req, container.get_id(), data, ll)
        try:
            obj_name = self.object_controller.object_name
            self.object_controller.object_name = container.get_id()
            resp = self.object_controller.store_container(req)
            self.object_controller.object_name = obj_name
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
            self._store_container(full_container)
        return container_id

    def _lazy_dedupe(self, fp, chunk):
        '''
        Deduplicate the chunk using the given fingerprint
        1. go through the bloom filer if unique, then unique, else, next to 2
        2. lookup in the cache, if found ,duplicate, else, next to 3
        3. buffer until one buffer bucket is full
        :param fp: fingerprint
        :param chunk: the content
        :return:
        '''
        if not fp in self.bf:
            self.bf.add(fp)
            container_id = self.store(fp, chunk)
            self.index.put(fp, container_id)
            return
        self.current_continue_dup += 1
        if self._get_from_cache(self.fp_cache, fp):
            return
        if self.current_continue_dup >= self.lazy_max_fetch:
            self.dup_cluster = dict()
            self.dup_cluster_set.append(self.dup_cluster)
            self.current_continue_dup = 0
        self.dup_cluster[fp] = chunk
        self.index.buf(fp, self.dup_cluster)

    def lazy_callback(self, result):
        load_container_set = set()
        for fp, container_id, clusters in result:
            if container_id and clusters:
                if container_id not in load_container_set:
                    self._fill_cache_with_container_fp(container_id)
                    load_container_set.add(container_id)
                for cluster in clusters:
                    for fp, chunk in cluster.items():
                        if self._get_from_cache(self.fp_cache, fp):
                            del cluster[fp]
            else:
                for cluster in clusters:
                    chunk = cluster.get(fp)
                    if chunk:
                        break
                container_id = self.store(fp, chunk)
                self.index.put(fp, container_id)
                for cluster in clusters:
                    del cluster[fp]
            for cluster in clusters:
                if not cluster:
                    self.dup_cluster_set.remove(cluster)

    def _eager_dedupe(self, fp, chunk):
        if fp in self.bf:
            if self._get_from_cache(self.fp_cache, fp):
                return
            container_id = self.index.get(fp)
            if container_id:
                self._fill_cache_with_container_fp(container_id)
                return
        self.bf.add(fp)
        container_id = self.store(fp, chunk)
        self.index.put(fp, container_id)

    def put(self, fp, chunk, obj_controller, req):
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

    def _eager_get(self, fp):
        r = self.container.get(fp)
        if r:
            return r
        for dc_id, dc in self.dc_cache.items():
            r = dc.get(fp)
            if r:
                return r
        dc_id = self.index.get(fp)

        req = self._dupl_req(self.req, dc_id, None, 0)

        obj_name = self.object_controller.object_name
        self.object_controller.object_name = dc_id
        resp = self.object_controller.GETorHEAD(req)
        self.object_controller.object_name = obj_name


        dc_container = DedupeContainer(dc_id)

        data = ''
        for d in iter(resp.app_iter):
            data += d
        if resp.headers.get('X-Object-Sysmeta-Compressed'):
            data = lz4.loads(data)
        dc_container.loads(data)

        self._add2cache(self.dc_cache, dc_id, dc_container)

        return dc_container.get(fp)

    def _lazy_get(self, fp):
        #check if the chunk in the buffer area
        for cluster in self.dup_cluster_set:
            r = cluster.get(fp)
            if r:
                return r
        return self._eager_get(fp)

    def get(self, fp, controller, req):
        self.object_controller = controller
        self.req = req
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