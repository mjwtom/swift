'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import os
from swift.dedupe.dedupe_container import DedupeContainer
import lz4


class RespBodyIter(object):
    def __init__(self, req, controller):
        self.controller = controller
        self.object_name = controller.object_name
        self.dedupe = controller.dedupe
        self.req = req
        self.resp = controller.GETorHEAD(req)
        self.fingerprints = ''
        for d in iter(self.resp.app_iter):
            self.fingerprints += d
        if self.resp.headers.get('X-Object-Sysmeta-Compressed'):
            self.fingerprints = lz4.loads(self.fingerprints)
        self.fp_number = len(self.fingerprints) / 32
        self.fp_cur = 0
        self.fp_size = 32
        self.req_environ_path = self.req.environ['PATH_INFO']

    def __iter__(self):
        return self

    def __next__(self):
        if self.fp_cur >= self.fp_number:
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fingerprint = self.fingerprints[self.fp_cur*self.fp_size:self.fp_cur*self.fp_size+self.fp_size]
            self.fp_cur += 1
        dedupe = self.dedupe
        dedupe.state.incre_download_chunk()
        container_id = dedupe.lookup(fingerprint)

        if container_id == str(dedupe.container_count):
            return dedupe.container.kv.get(fingerprint, None)

        dc_container = dedupe.DCFromCache(container_id)
        if dc_container:
            return dc_container.kv.get(fingerprint, None)

        l = len(self.object_name)
        tmp_pth = self.req_environ_path[:-l]
        self.req.environ['PATH_INFO'] = tmp_pth+ str(container_id)

        dc_container = DedupeContainer(container_id)

        self.controller.object_name = container_id
        resp = self.controller.GETorHEAD(self.req)
        data = ''
        for d in iter(resp.app_iter):
            data += d
        if resp.headers.get('X-Object-Sysmeta-Compressed'):
            data = lz4.loads(data)
        dc_container.loads(data)

        dedupe.DC2Cache(container_id, dc_container)

        return dc_container.kv.get(fingerprint, None)

    def next(self):
        return self.__next__()

    def get_resp(self):
        return self.resp