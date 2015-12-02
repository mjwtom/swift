import time


class DedupeState(object):
    def __init__(self, time_gap = 10):
        self.total_chunk = 0
        self.dupe_chunk = 0
        self.total_size = 0
        self.dupe_size = 0
        self.total_time = 0
        self.cur_size = 0
        self.time_gap = time_gap
        self.last_print_time = time.time()

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

    def print_state(self):
        print('total data size: %d, duplicate size: %d, total chunk: %d, duplicate chunk: %d\n' % (self.total_size,
                                                                                                   self.dupe_size,
                                                                                                   self.total_chunk,
                                                                                                   self.dupe_chunk))
