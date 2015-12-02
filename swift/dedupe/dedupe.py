__author__ = 'mjwtom'

from swift.dedupe.fp_index import fp_index
from swift.dedupe.cache import DedupeCache
from swift.dedupe.pybloom.pybloom import BloomFilter
from swift.dedupe.fingerprint import fingerprint
from swift.dedupe.dedupe_container import DedupeContainer
from swift.dedupe.state import DedupeState


class dedupe(object):
    def __init__(self, conf):
        self.fp_cache = DedupeCache(int(conf.get('cache_size', 65536)))
        self.dc_cache = DedupeCache(int(conf.get('dc_cache', 4)))
        self.index = fp_index(conf.get('data_base', ':memory:'))
        self.bf = BloomFilter(int(conf.get('bf_capacity', 1024*1024)))
        self.fixed_chunk = bool(conf.get('fixed_chunk', False))
        self.dc_size = int(conf.get('dedupe_container_size', 4096))
        self.container_count = 0
        self.container = DedupeContainer(str(self.container_count), self.dc_size)
        self.state = DedupeState()

    def lookup(self, key):
        if not (key in self.bf):
            return None
        ret = self.fp_cache.get(key)
        if ret:
            return ret
        ret = self.index.lookup_fp_index(key)
        return ret

    def insert_fp_index(self, fp, container_id):
        self.bf.add(fp)
        self.index.insert_fp_index(fp, container_id)

    def insert_obj_fps(self, obj_hash, fps):
        self.index.insert_obj_fps(obj_hash, fps)

    def load2cache(self, fingerprints):
        for f in fingerprints:
            self.fp_cache.put(f, '0000')

    def DCFromCache(self, key):
        dc = self.dc_cache.get(key)
        return dc

    def DC2Cache(self, key, dc):
        self.dc_cache.put(key, dc)

    def hash(self, data):
        return fingerprint(data)