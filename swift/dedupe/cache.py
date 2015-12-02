__author__ = 'mjwtom'

from swift.dedupe.repoze.lru import LRUCache


class DedupeCache(object):
    def __init__(self, size):
        self.lrucache = LRUCache(size)

    def put(self, key, value):
        self.lrucache.put(key, value)

    def get(self, key):
        return self.lrucache.get(key)