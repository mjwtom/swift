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

    def time(self):
        return datetime.now()

    def time_diff(self, start, end):
        diff = end -start
        return diff.total_seconds()

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
                'hit compressed num: %d' % self.hit_compressed]
        return info

    def get_penalty(self, container, compress = None):
        mem_size = sys.getsizeof(container.kv)
        orig_data = ''
        for k, v in container.kv.items():
            orig_data += v
        orig_size = len(orig_data)
        dump_data = container.dumps()
        dump_size = len(dump_data)
        if compress:
            orig_compressed_data = compress(orig_data)
            orig_compressed_size = len(orig_compressed_data)
            compress_dump_data = compress(dump_data)
            compress_dump_size = len(compress_dump_data)

        info = dict(
            mem_size = mem_size,
            orig_size = orig_size,
            dump_size = dump_size,
        )
        if compress:
            info.update({'orig_compressed_size': orig_compressed_size,
                         'compress_dump_size': compress_dump_size})

        text = []
        for k, v in info.items():
            text.append('%s: %d' % (k, v))
        return text
