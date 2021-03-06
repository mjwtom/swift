from datetime import datetime
import sys


class DedupeSummary(object):
    def __init__(self):
        self.total_chunk = 0
        self.dupe_chunk = 0
        self.total_size = 0
        self.dupe_size = 0
        self.total_download = 0
        self.total_download_chunk = 0
        self.chunk_time = 0
        self.hash_time = 0
        self.fp_lookup_time = 0
        self.store_time = 0
        self.dc_num = 0
        self.compression_time = 0
        self.decompression_time = 0
        self.hit_uncompressed = 0
        self.hit_compressed = 0
        self.get = 0
        self.get_chunk_time = 0
        self.get_recipe_time = 0
        self.get_cid_time = 0
        self.container_pickle_dumps_time = 0
        self.container_pickle_loads_time = 0
        self.pre_cache_hit = 0
        self.post_cache_hit = 0
        self.disk_hit = 0
        self.read_disk_num = 0
        self.read_disk_time = 0
        self.write_disk_num = 0
        self.write_disk_time = 0
        self.lazy_calback_time = 0
        self.retrieve_time = 0

    def get_info(self):
        self.total_download_chunk = self.get
        info = ['total size: %d' % self.total_size,
                'total chunk: %d' % self.total_chunk,
                'duplicate size: %d' % self.dupe_size,
                'duplicate chunk: %d' % self.dupe_chunk,
                'total download size: %d' % self.total_download,
                'total download chunk: %d' % self.total_download_chunk,
                'chunking time: %f seconds' % self.chunk_time,
                'hashing time: %f seconds' % self.hash_time,
                'fingerprint lookup time: %f seconds' % self.fp_lookup_time,
                'store time: %f seconds' % self.store_time,
                'deduplication container num: %d' % self.dc_num,
                'compression time: %f seconds' % self.compression_time,
                'decompression time: %f seconds' % self.decompression_time,
                'get num: %d' % self.get,
                'hit uncompressed num: %d' % self.hit_uncompressed,
                'hit compressed num: %d' % self.hit_compressed,
                'get chunk time: %f' % self.get_chunk_time,
                'get recipe time: %f' % self.get_recipe_time,
                'get chunk container id time: %f' % self.get_cid_time,
                'container pickle dumps time: %f' % self.container_pickle_dumps_time,
                'container pickle loads time: %f' % self.container_pickle_loads_time,
                'pre cache hit num: %d' % self.pre_cache_hit,
                'post cache hit num: %d' % self.post_cache_hit,
                'disk hit num: %d' % self.disk_hit,
                'write disk num: %d' % self.write_disk_num,
                'write disk time: %f' % self.write_disk_time,
                'read disk num: %d' % self.read_disk_num,
                'read disk time: %f' % self.read_disk_time,
                'lazy callback time: %f' % self.lazy_calback_time,
                'retrieve container time: %f' % self.retrieve_time]
        return info
