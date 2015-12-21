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


class ChunkStore(object):
    def __init__(self, conf, app):
        self.app = app
        self.object_controller = None
        self.req = None
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
        self.sqlite_index = config_true_value(conf.get('sqlite_index', 'false'))
        if self.sqlite_index:
            self.index = DatabaseTable(conf)
        else:
            self.lazy_dedupe = config_true_value(conf.get('lazy_dedupe', 'true'))
            if self.lazy_dedupe:
                self.index = LazyHashTable(conf)
                self.lazy_max_unique = int(conf.get('lazy_max_unique', 200))
                self.lazy_max_fetch = int(conf.get('lazy_max_fetch', self.dc_size))
            else:
                self.index = DiskHashTable(conf)

    def lookup(self, key):
        if not (key in self.bf):
            return None
        ret = self.fp_cache.get(key)
        if ret:
            return ret
        ret = self.index.lookup_fp(key)
        return ret

    def insert_fp(self, fp, container_id):
        self.bf.add(fp)
        self.index.insert_fp(fp, container_id)

    def insert_obj_fps(self, obj_hash, fps):
        self.index.insert_obj_fps(obj_hash, fps)

    def load2cache(self, fingerprints):
        for f in fingerprints:
            self.fp_cache.put(f, '0000')

    def _add2cache(self, cache, key, value):
        cache[key] = value

    def _get_from_cache(self, cache, key):
        return cache[key]

    def hash(self, data):
        return md5(data).hexdigest()

    def new_container(self):
        self.container = DedupeContainer(str(self.next_dc_id), self.dc_size)
        self.next_dc_id += 1

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
            self.container.add(fp, chunk)
            return

    def _eager_dedupe(self, fp, chunk):
        if fp in self.bf:
            if self.fp_cache[fp]:
                return
            if self.index.get(fp):
                return
        self.bf.add(fp)
        self.container.add(fp, chunk)
        container_id = self.container.get_id()
        self.index.add(fp, container_id)
        if self.container.is_full():
            full_container = self.container
            self.new_container()
            self._store_container(full_container)

    def put(self, fp, chunk, obj_controller, req):
        self.object_controller = obj_controller
        self.req = req
        if self.lazy_dedupe:
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
        pass

    def get(self, fp, controller, req):
        self.object_controller = controller
        self.req = req
        if self.sqlite_index:
            return self._sqlite_get(fp)
            pass # TODO: get a chunk in lazy deduplication
        elif self.lazy_dedupe:
            return self._lazy_get(fp)
        else:
            return self._eager_get(fp)

    def get_coutainer_id(self, fp):
        pass

    def put_in_lazy_buffer(self, fp, ):
        #TODO: lazy method implementation
        pass


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