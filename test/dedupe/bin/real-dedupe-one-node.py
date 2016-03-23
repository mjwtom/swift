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

network_throughput = 60.0
compression_rate = 0.32

target_size = 4096
fixed_size = True


class TestStore(ChunkStore):
    def __init__(self, conf):
        ChunkStore.__init__(self, conf=conf, app=None)

    def put(self, fp, chunk):
        ChunkStore.put(self, fp, chunk, None, None)

    def _store_container(self, container, delay=write_delay):
        if not delay:
            return
        data = container.dumps()
        l = len(data)
        trans_time = 1.0*l/1024/1024/network_throughput
        sleep(trans_time)

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
    data_source = readfile(f)
    chunker = ChunkIterC(data_src=data_source,
                         fixed_size=fixed_size,
                         target=target_size)
    for chunk in chunker:
        fp = md5(chunk).hexdigest()
        chunk_store.put(fp, chunk)


def test_put(chunk_store, pickle_path):
    if not os.path.exists(pickle_path):
        print 'no fingerprint file'
        return
    fin = open(pickle_path, 'rb')
    files = pickle.load(fin)
    fin.close()
    for f in files:
        dedupe(f, chunk_store)

        print 'At last, we give the result'
        show_stat(chunk_store)
        end_time = time()
        time_gap = time_diff(start_time, end_time)
        if time_gap > 0.0:
            print ('throughput %f MB/s' % (file_size*1.0/1024/1024/time_gap))


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
