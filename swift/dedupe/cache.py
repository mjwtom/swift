from hashlib import md5
class DeDuplicationCache(object):
    def __init__(self, index_size = 1024*128, bucket_size = 16):
        self.index_size = index_size
        self.bucket_size = bucket_size
        self.bucket = []
        self.queue = []
        for _ in range(self.index_size):
            self.bucket.append(dict())
            self.queue.append([])

    def _map_bucket(self, key):
        h = md5(key)
        h = h.hexdigest()
        k = int(h.upper(), 16)
        k %= self.index_size
        return k

    def put(self, key, value):
        k = self._map_bucket(key)
        if len(self.bucket[k]) < self.bucket_size:
            old_value = self.bucket[k].get(key, None)
            if not old_value:
                self.bucket[k][key] = value
                self.queue[k].append(key)
            else:
                if not (old_value == value):
                    self.bucket[k][key] = value
        else:
            pass

    def get(self, key):
        return self.lrucache.get(key)