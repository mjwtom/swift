from datetime import datetime


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

    def start_time(self):
        return datetime.now()

    def end_time(self):
        return datetime.now()

    def time_diff(self, start, end):
        diff = end -start
        return diff.total_seconds()

    def get_info(self):
        info = ['total size: %d' % self.total_size,
                'total chunk: %d' % self.total_chunk,
                'duplicate size: %d' % self.dupe_size,
                'duplicate chunk: %d' % self.dupe_chunk,
                'total download size: %d' % self.total_download,
                'total download chunk: %d' % self.total_download_chunk,
                'chunking time: %f seconds' % self.chunk_time,
                'hashing time: %f seconds' % self.hash_time,
                'fingerprint lookup time: %f seconds' % self.fp_lookup_time,
                'store time: %d microseconds' % self.store_time,
                'deduplication container num: %d' % self.dc_num,
                'compression time: % d microseconds' % self.compression_time]
        return info
