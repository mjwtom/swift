import time


class DedupeSummary(object):
    def __init__(self):
        self.total_chunk = 0
        self.dupe_chunk = 0
        self.total_size = 0
        self.dupe_size = 0
        self.total_download = 0
        self.total_download_chunk = 0

    def add_dup_size(self, size):
        self.dupe_size += size

    def add_total_size(self, size):
        self.total_size += size

    def incre_dupe_chunk(self):
        self.dupe_chunk += 1

    def incre_total_chunk(self):
        self.total_chunk += 1

    def incre_total_download(self, size):
        self.total_download += size

    def incre_download_chunk(self):
        self.total_download_chunk += 1

    def get_info(self):
        info = ['total size: %d' % self.total_size,
                'total chunk: %d' % self.total_chunk,
                'duplicate size: %d' % self.dupe_size,
                'duplicate chunk: %d' % self.dupe_chunk,
                'total download size %d' % self.total_download,
                'total download chunk %d' % self.total_download_chunk]
        return info
