from swift.common.utils import ContextPool, Timestamp
from swift.common.exceptions import ConnectionTimeout, DiskFileQuarantined, \
    DiskFileNotExist, DiskFileCollision, DiskFileNoSpace, DiskFileDeleted, \
    DiskFileDeviceUnavailable, DiskFileExpired, ChunkReadTimeout, \
    DiskFileXattrNotSupported
from swift.obj.diskfile import DATAFILE_SYSTEM_META, DiskFileRouter
from swift.common.utils import public, get_logger
import lz4
from hashlib import md5
from time import time
from eventlet.queue import Queue
import threading

'''
use lz4 downloaded from https://github.com/steeve/python-lz4
'''


class Compress(threading.Thread):
    def __init__(self, conf, logger = None, name='compressor', thread_num=16, queue_len = 50):
        self.thread_num = thread_num
        self.pool = ContextPool(self.thread_num)
        self.queue = Queue()
        self.logger = logger or get_logger(conf, log_route='object-server')
        self._diskfile_router = DiskFileRouter(conf, self.logger)
        super(Compress, self).__init__(name = name)

    def get_diskfile(self, device, partition, account, container, obj,
                     policy, **kwargs):
        """
        Utility method for instantiating a DiskFile object supporting a given
        REST API.

        An implementation of the object server that wants to use a different
        DiskFile class would simply over-ride this method to provide that
        behavior.
        """
        return self._diskfile_router[policy].get_diskfile(
            device, partition, account, container, obj, policy, **kwargs)

    def _get(self, info):
        device = info.get('device')
        partition = info.get('partition')
        account = info.get('account')
        container = info.get('container')
        object = info.get('object')
        policy = info.get('policy')
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, object,
                policy=policy)
        except DiskFileDeviceUnavailable:
            return None
        metadata = ''
        data = ''
        with disk_file.open():
            metadata = disk_file.get_metadata()
            reader = disk_file.reader()
            data = ''
            for d in reader:
                data += d
        return metadata, data

    def _delete(self, info):
        device = info.get('device')
        partition = info.get('partition')
        account = info.get('account')
        container = info.get('container')
        object = info.get('object')
        policy = info.get('policy')
        timestamp = Timestamp(info.get('X-Timestamp', 0))

        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, object,
                policy=policy)
        except DiskFileDeviceUnavailable:
            return False

        disk_file.delete(timestamp)
        return True

    def _put(self, info, metadata, data):
        device = info.get('device')
        partition = info.get('partition')
        account = info.get('account')
        container = info.get('container')
        object = info.get('object')
        policy = info.get('policy')
        fsize = len(data)
        frag_index = metadata.get('frag_index')
        timestamp = Timestamp(metadata.get('X-Timestamp', '0'))
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, object,
                policy=policy, frag_index=frag_index)
        except DiskFileDeviceUnavailable:
            return False
        try:
            with disk_file.create(size=fsize) as writer:
                writer.write(data)
                writer.put(metadata)
                writer.commit(timestamp)
        except (DiskFileXattrNotSupported, DiskFileNoSpace):
            return False
        return True

    def compress_file(self, info):
        metadata, data = self._get(info)
        data = lz4.dumps(data)
        metadata['Content-Length'] = str(len(data))
        metadata['Compressed'] = 'True'
        info['X-Timestamp'] = metadata.get('X-Timestamp', '0')
        self._delete(info)
        metadata['X-Timestamp'] = Timestamp(time()).internal
        self._put(info, metadata, data)

    def run(self):
        while True:
            info = self.queue.get()
            self.compress_file(info)#self.pool.spawn(self.compress_file, info)
        self.queue.task_done()

    def put_into_queue(self, info):
        self.queue.put(info)