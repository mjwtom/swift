__author__ = 'mjwtom'

from swift.dedupe.fp_index import fp_index
from swift.dedupe.cache import mycache

class dedupe(object):

    def __init__(self, conf):
        self.cache = mycache(int(conf.get('cache_size', 65536)))
        self.index = fp_index(conf.get('data_base', ':memory:'))

    def lookup(self, key):
        ret = self.index.lookup_fp_index(key)
        return ret

    def insert_fp_index(self, key, value, obj_hash):
        self.index.insert_fp_index(key, value, obj_hash)

    def insert_obj_fps(self, obj_hash, fps):
        self.index.insert_obj_fps(obj_hash, fps)
