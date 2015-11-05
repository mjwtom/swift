'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import os
import binascii
from swift import gettext_ as _
from swift.common.storage_policy import POLICIES
from swift.dedupe.dedupe_container import dedupe_container


class RespBodyIter(object):
    def __init__(self, req, controller):
        self.controller = controller
        self.req = req
        self.resp = controller.GETorHEAD(req)
        self.fingerprints = self.resp.body
        # self.fp_number = len(self.fingerprints) / 16
        self.fp_number = len(self.fingerprints) / 32
        self.fp_cur = 0
        self.fp_size = 32
        self.req_environ_path = self.req.environ['PATH_INFO']

    def __iter__(self):
        return self

    def __next_chunk__(self):
        if(self.fp_cur >= self.fp_number):
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fingerprint = self.fingerprints[self.fp_cur*self.fp_size:self.fp_cur*self.fp_size+self.fp_size]
            self.fp_cur += 1

        self.req.environ['PATH_INFO'] = os.path.dirname(self.req_environ_path)
        self.req.environ['PATH_INFO'] = self.req.environ['PATH_INFO'] + '/' + fingerprint

        self.controller.object_name = fingerprint
        resp = self.controller.GETorHEAD(self.req)
        return resp.body

    def __next__(self):
        if(self.fp_cur >= self.fp_number):
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fingerprint = self.fingerprints[self.fp_cur*self.fp_size:self.fp_cur*self.fp_size+self.fp_size]
            self.fp_cur += 1
        dedupe = self.controller.dedupe
        container_id = dedupe.lookup()

        if container_id == str(dedupe.container_count):
            return dedupe.container.kv[fingerprint]

        self.req.environ['PATH_INFO'] = os.path.dirname(self.req_environ_path)
        self.req.environ['PATH_INFO'] = self.req.environ['PATH_INFO'] + '/' + fingerprint

        container = dedupe_container(container_id)

        self.controller.object_name = fingerprint
        resp = self.controller.GETorHEAD(self.req)
        container.frombyte(resp.body)
        return container.kv[fingerprint]

    def next(self):
        return self.__next__()

    def get_resp(self):
        return self.resp