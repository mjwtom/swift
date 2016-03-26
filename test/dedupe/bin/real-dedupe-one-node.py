import os
from swift.dedupe.deduplication import ChunkStore
from swift.common.utils import parse_options
from swift.common.wsgi import appconfig, ConfigFileError
from time import sleep
from swift.dedupe.time import time, time_diff
from swift.dedupe.compress import compress
import pickle
from swift.dedupe.chunk import ChunkIterC
from hashlib import md5


# To simulate the store and read process
write_delay = False
read_delay = False
compress_container = True

target_size = 4096
fixed_size = True

total_size = 0


class TestStore(ChunkStore):
    def __init__(self, conf):
        ChunkStore.__init__(self, conf=conf, app=None)
        self.total_size = 0

    def put(self, fp, chunk):
        ChunkStore.put(self, fp, chunk, None, None)

    def _store_container(self, container, delay=write_delay):
        if compress_container:
            data = container.dumps()
            data = compress(data, 'lz4hc')
            self.total_size += len(data)

    def get(self, fp):
        pass


def readfile(path, readsize=4096*1024):
    if not os.path.exists(path):
        print 'no such file'
    with open(path, 'rb') as f:
        data = f.read(readsize)
        while data:
            yield data
            data = f.read(readsize)
        else:
            return


def show_stat(chunk_store):
    chunk_store.summary.read_disk_num = chunk_store.index.read_disk_num
    chunk_store.summary.read_disk_time = chunk_store.index.read_disk_time
    chunk_store.summary.write_disk_num = chunk_store.index.write_disk_num
    chunk_store.summary.write_disk_time = chunk_store.index.write_disk_time
    chunk_store.summary.disk_hit = chunk_store.index.hit_num
    info = chunk_store.summary.get_info()
    for entry in info:
        print entry
    chunk_store.log_message(info)


def dedupe(f, chunk_store):
    fps = []
    data_source = readfile(f)
    chunker = ChunkIterC(data_src=data_source,
                         fixed_size=fixed_size,
                         target=target_size)
    for chunk in chunker:
        fp = md5(chunk).hexdigest()
        chunk_store.put(fp, chunk)
        fps.append(fp)
    data = pickle.dumps(fps)
    data = compress(data, 'lz4hc')
    chunk_store.total_size += len(data)


def test_put(chunk_store, pickle_path):
    if not os.path.exists(pickle_path):
        print 'no fingerprint file'
        return
    fin = open(pickle_path, 'rb')
    files = pickle.load(fin)
    fin.close()
    for f in files:
        print 'now processing %s' % f
        dedupe(f, chunk_store)
        show_stat(chunk_store)

        print 'total deduplicated size %d' % chunk_store.total_size

    print 'at last, total deduplicated size %d' % chunk_store.total_size
    show_stat(chunk_store)


def start_test():
    try:
        conf = appconfig(conf_file, name='proxy-server')
    except Exception as e:
        raise ConfigFileError("Error trying to load config from %s: %s" %
                              (conf_file, e))
    chunk_store = TestStore(conf)
    test_put(chunk_store, pickle_file)


if __name__ == '__main__':
    conf_file, options = parse_options()
    extra_args = options.get('extra_args')
    pickle_file = extra_args[0]
    start_test()
