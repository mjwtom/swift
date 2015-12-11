from swift.common.utils import ContextPool, Timestamp
from swift.common.exceptions import ConnectionTimeout, DiskFileQuarantined, \
    DiskFileNotExist, DiskFileCollision, DiskFileNoSpace, DiskFileDeleted, \
    DiskFileDeviceUnavailable, DiskFileExpired, ChunkReadTimeout, \
    DiskFileXattrNotSupported
import lz4
from hashlib import md5

'''
use lz4 downloaded from https://github.com/steeve/python-lz4
'''

class Compress(object):
    def __init__(self, thread_num=16):
        self.thread_num = thread_num
        self.pool = ContextPool(self.thread_num)

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
        disk_file.open()
        metadata = disk_file.get_metadata()
        size = int(metadata['Content-Length'])
        data = disk_file.reader.read(size)
        disk_file.close()
        return data

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

    def _put(self, info, data, metadata):
        device = info.get('device')
        partition = info.get('partition')
        account = info.get('account')
        container = info.get('container')
        object = info.get('object')
        policy = info.get('policy')
        fsize = len(data)
        metadata = info.get('metadata')
        frag_index = metadata.get('frag_index')
        orig_timestamp = Timestamp(metadata.get('X-Timestamp', 0))
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, object,
                policy=policy, frag_index=frag_index)
        except DiskFileDeviceUnavailable:
            return False
        etag = md5()
        try:
            with disk_file.create(size=fsize) as writer:
                etag.update(data)
                writer.write(data)
                writer.put(metadata)

        except (DiskFileXattrNotSupported, DiskFileNoSpace):
            return False
        return True

    def compress_file(self, info):
        data = self._get(info)
        data = lz4.dumps(data)
        self._delete(info)
        self._put(info, data)