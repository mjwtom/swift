import os
from swift.dedupe.deduplication import ChunkStore
from swift.common.utils import parse_options
from swift.common.wsgi import appconfig, ConfigFileError
from time import sleep
from swift.dedupe.time import time, time_diff
import six.moves.cPickle as pickle
from directio import read, write
from swift.dedupe.dedupe_container import DedupeContainer


test_upload_file_num = 30
test_download_file_num = 30

# To simulate the store and read process
compress = False
decompress = False
async_compress = False
write_delay = False
read_delay = False


network_throughput = 60.0
compress_rate = 0.32
compress_speed = 13
decompress_speed = 1200


class TestStore(ChunkStore):
    def __init__(self, conf):
        ChunkStore.__init__(self, conf=conf, app=None)
        self.fake_container = dict()

    def put(self, fp, chunk):
        ChunkStore.put(self, fp, chunk, None, None)

    def get(self, fp):
        return ChunkStore.get(self, fp, None, None)

    def _store_container(self, container):
        store_start = time()
        data = container.dumps()
        l = len(data)
        compress_start = time()
        if compress:
            if not async_compress:
                delay_time = 1.0*l/1024/1024/compress_speed
                sleep(delay_time)
        compress_end = time()
        self.summary.compression_time += time_diff(compress_start, compress_end)
        if write_delay:
            l *= compress_rate
            trans_time = 1.0*l/1024/1024/network_throughput
            sleep(trans_time)
        store_end = time()
        self.summary.store_time += time_diff(store_start, store_end)

    def _store_container_fp(self, container):
        if not os.path.exists(self.container_fp_dir):
            os.makedirs(self.container_fp_dir)
        path = self.container_fp_dir + '/' + container.get_id()
        fps = container.get_fps_lens()
        data = pickle.dumps(fps)
        if self.direct_io:
            f = os.open(path, os.O_CREAT | os.O_APPEND | os.O_RDWR | os.O_DIRECT)
            len_data = len(data)
            if (len_data % 512) != 0:
                ll = 512 - len_data%512 # alligned by 512
                data += '\0'*ll
            try:
                write(f, data)
            except Exception as e:
                pass
            finally:
                os.close(f)
        else:
            with open(path, 'wb') as f:
                pickle.dump(fps, f)

    def _load_container_fp(self, container_id):
        if container_id == self.container.get_id():
            fps = self.container.get_fps_lens()
            return fps
        path = self.container_fp_dir + '/' + container_id
        if not os.path.exists(path):
            return None
        if self.direct_io:
            ll = os.path.getsize(path)
            f = os.open(path, os.O_RDONLY | os.O_DIRECT)
            try:
                data = read(f, ll)
                fps = pickle.loads(data)
            except Exception as e:
                pass
            finally:
                os.close(f)
        else:
            with open(path, 'rb') as f:
                fps = pickle.load(f)
        return fps

    def _fill_cache_with_container_fp(self, container_id):
        fps_lens = self._load_container_fp(container_id)
        print 'load container id %s' % container_id
        for fp, l in fps_lens:
            self._add2cache(self.fp_cache, fp, container_id)

    def retrieve_container(self, container_id):
        dedupe_start = time()
        path = self.container_fp_dir + '/' + container_id
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            data = f.read()
        kv = pickle.loads(data)
        size = 0
        for _, l in kv:
            size += l
        size *= compress_rate
        self.container_pool[container_id] = (size, kv)
        if read_delay:
            delay_time = 1.0*size/1024/1024/network_throughput
            sleep(delay_time)
        dedupe_end = time()
        self.summary.retrieve_time += time_diff(dedupe_start, dedupe_end)
        container = self.get_container_from_compressed_data(data, container_id)
        return container

    def get_container_from_compressed_data(self, data, dc_id):
        dedupe_start = time()
        size, kv = self.container_pool[dc_id]
        if decompress:
            delay_time = 1.0*size/1024/1024/decompress_speed
            sleep(delay_time)
        dc_container = DedupeContainer(dc_id)
        for fp, l in kv:
            chunk = get_str(l)
            dc_container.add(fp, chunk)
        dedupe_end = time()
        self.summary.decompression_time += time_diff(dedupe_start, dedupe_end)
        return dc_container


def get_size(finger_path):
    with open(finger_path, 'r') as f:
        line = f.readline()
        while line:
            name = line
            total_size = 0
            line = f.readline()
            while line and (not line.startswith('/')):
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
    file_num = 0
    if not os.path.exists(finger_path):
        print 'no fingerprint file'
        return
    with open(finger_path, 'r') as fp_in:
        file_size = 0
        start_time = time()
        for line in fp_in:
            if line.startswith('/'):
                if file_num >= test_upload_file_num:
                    break
                file_num += 1
                print 'file %s' % line
                show_stat(chunk_store)
                end_time = time()
                time_gap = time_diff(start_time, end_time)
                if time_gap > 0.0:
                    print ('put throughput %f MB/s' % (file_size*1.0/1024/1024/time_gap))
                start_time = end_time
                file_size = 0
                continue
            # print chunk_store.summary.total_chunk
            line = line.strip()
            data = line.split()
            fp = data[0]
            size = int(data[1])
            data = get_str(size)
            file_size += size
            if write_delay:
                delay_time = 1.0*size/1024/1024/network_throughput
                sleep(delay_time)
            chunk_store.put(fp, data)

        print 'At last, we give the result'
        show_stat(chunk_store)
        end_time = time()
        time_gap = time_diff(start_time, end_time)
        if time_gap > 0.0:
            print ('put throughput %f MB/s\n' % (file_size*1.0/1024/1024/time_gap))


def test_get(chunk_store, finger_path):
    file_num = 0
    if not os.path.exists(finger_path):
        print 'no fingerprint file'
        return
    with open(finger_path, 'r') as fp_in:
        file_size = 0
        start_time = time()
        for line in fp_in:
            if line.startswith('/'):
                if file_num >= test_download_file_num:
                    break
                file_num += 1
                print 'file %s' % line
                show_stat(chunk_store)
                end_time = time()
                time_gap = time_diff(start_time, end_time)
                if time_gap > 0.0:
                    print ('get throughput %f MB/s' % (file_size*1.0/1024/1024/time_gap))
                start_time = end_time
                file_size = 0
                continue
            #print chunk_store.summary.total_chunk
            line = line.strip()
            data = line.split()
            fp = data[0]
            size = int(data[1])
            file_size += size
            chunk = chunk_store.get(fp)
            if read_delay:
                delay_time = 1.0*size/1024/1024/network_throughput
                sleep(delay_time)

        print 'At last, we give the result'
        show_stat(chunk_store)
        end_time = time()
        time_gap = time_diff(start_time, end_time)
        if time_gap > 0.0:
            print ('get throughput %f MB/s\n' % (file_size*1.0/1024/1024/time_gap))


def start_test():
    try:
        conf = appconfig(conf_file, name='proxy-server')
    except Exception as e:
        raise ConfigFileError("Error trying to load config from %s: %s" %
                              (conf_file, e))
    chunk_store = TestStore(conf)
    print 'start to test write'
    test_put(chunk_store, finger_path)
    print 'start to test read'
    test_get(chunk_store, finger_path)


if __name__ == '__main__':
    conf_file, options = parse_options()
    extra_args = options.get('extra_args')
    finger_path = extra_args[0]
    start_test()
