import os
from swift.dedupe.deduplication import ChunkStore
from swift.common.utils import parse_options
from swift.common.wsgi import appconfig, ConfigFileError
from time import sleep
from swift.dedupe.time import time, time_diff


# To simulate the store and read process
network_throughput = 60.0
compression_rate = 0.32


class TestStore(ChunkStore):
    def __init__(self, conf):
        ChunkStore.__init__(self, conf=conf, app=None)

    def put(self, fp, chunk):
        ChunkStore.put(self, fp, chunk, None, None)

    def _store_container(self, container, delay=True):
        if not delay:
            return
        data = container.dumps()
        l = len(data)
        trans_time = 1.0*l/1024/1024/network_throughput
        sleep(trans_time)

    def get(self, fp):
        pass


def get_size(finger_path):
    with open(finger_path, 'r') as f:
        line = f.readline()
        while line:
            name = line
            total_size = 0
            line = f.readline()
            while line and (not line.startswith('/home/')):
                size = line.split()
                size = int(size[1])
                total_size += size
                line = f.readline()
            else:
                total_size = float(total_size)/1024/1024
                print 'file %s has total length: %d' % (name, total_size)


def get_str(length=8, random=False):
    if not random:
        data = 'x'*length
        return data
    else:
        pass


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


def test_put(chunk_store, finger_path):
    if not os.path.exists(finger_path):
        print 'no fingerprint file'
        return
    with open(finger_path, 'r') as fp_in:
        file_size = 0
        start_time = time()
        for line in fp_in:
            if line.startswith('/home/'):
                print 'file %s' % line
                show_stat(chunk_store)
                end_time = time()
                time_gap = time_diff(start_time, end_time)
                if time_gap > 0.0:
                    print ('throughput %f MB/s' % (file_size*1.0/1024/1024/time_gap))
                start_time = end_time
                file_size = 0
                continue
            #print chunk_store.summary.total_chunk
            line = line.strip()
            data = line.split()
            fp = data[0]
            size = int(data[1])
            data = get_str(size)
            file_size += size
            chunk_store.put(fp, data)

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
    test_put(chunk_store, finger_path)


if __name__ == '__main__':
    conf_file, options = parse_options()
    extra_args = options.get('extra_args')
    finger_path = extra_args[0]
    start_test()
