__author__ = 'mjwtom'

from swift.dedupe.fp_index import fp_index
from swift.dedupe.cache import mycache
from swift.dedupe.pybloom.pybloom import BloomFilter
from swift.dedupe.fingerprint import fingerprint
from swift.dedupe.dedupe_container import dedupe_container


class dedupe(object):
    def __init__(self, conf):
        self.cache = mycache(int(conf.get('cache_size', 65536)))
        self.index = fp_index(conf.get('data_base', ':memory:'))
        self.bf = BloomFilter(int(conf.get('bf_capacity', 1024*1024)))
        self.fixed_chunk = bool(conf.get('fixed_chunk', False))
        self.dedupe_container_size = int(conf.get('dedupe_container_size', 4096))
        self.container_count = 0
        self.container = dedupe_container(str(self.container_count), self.dedupe_container_size)

    def lookup(self, key):
        if not (key in self.bf):
            return None
        ret = self.cache.get(key)
        if ret:
            return ret
        ret = self.index.lookup_fp_index(key)
        return ret

    def insert_fp_index(self, key, value, obj_hash):
        self.bf.add(key)
        self.index.insert_fp_index(key, value, obj_hash)

    def insert_obj_fps(self, obj_hash, fps):
        self.index.insert_obj_fps(obj_hash, fps)

    def load2cache(self, fingerprints):
        for f in fingerprints:
            self.cache.put(f, '0000')

    def hash(self, data):
        return fingerprint(data)