import os
from swift.dedupe.deduplication import ChunkStore
from swift.common.utils import parse_options
from swift.common.wsgi import appconfig, ConfigFileError
from time import sleep


# To simulate the store and read process
store_container_time = 0.001


class TestStore(ChunkStore):
    def __init__(self, conf):
        ChunkStore.__init__(self, conf=conf, app=None)

    def put(self, fp, chunk):
        ChunkStore.put(self, fp, chunk, None, None)

    def _store_container(self, container):
        sleep(store_container_time)

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


def test_put(chunk_store, finger_path):
    if not os.path.exists(finger_path):
        print 'no fingerprint file'
        return
    with open(finger_path, 'r') as fp_in:
        for line in fp_in:
            if line.startswith('/home/'):
                print 'file %s' % line
                continue
            #print chunk_store.summary.total_chunk
            line = line.strip()
            data = line.split()
            fp = data[0]
            size = int(data[1])
            data = get_str(size)
            chunk_store.put(fp, data)


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
