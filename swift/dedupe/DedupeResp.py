'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import os
from swift.proxy.controllers.base import Controller
from swift import gettext_ as _

class RespBodyIter(Controller):
    def __init__(self, app, req, obj_ring, account_name, container_name, object_name):
        Controller.__init__(self, app)
        self.req = req
        self.obj_ring = obj_ring
        self.account_name = account_name
        self.container_name = container_name
        self.object_name = object_name
        self.req_environ_path = req.environ['PATH_INFO']

        partition = self.obj_ring.get_part(
            self.account_name, self.container_name, self.object_name)
        resp = self.GETorHEAD_base(
            req, _('Object'), self.obj_ring, partition,
            req.swift_entity_path)
        self.fingerprints = resp.body
        self.fp_number = len(self.fingerprints) / 16
        self.fp_cur = 0

    def __iter__(self):
        return self

    def __next__(self):
        if(self.fp_cur >= self.fp_number):
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fingerprint = self.fingerprints[self.fp_cur*16:self.fp_cur*16+16]
            self.fp_cur += 1
            str_fingerprint = ''
            for a in fingerprint:
                str_fingerprint += hex(ord(a))[2:]

        self.req.environ['PATH_INFO'] = os.path.dirname(self.req_environ_path)
        self.req.environ['PATH_INFO'] = self.req.environ['PATH_INFO'] + '/' + str_fingerprint

        partition = self.obj_ring.get_part(
            self.account_name, self.container_name, str_fingerprint)
        resp = self.GETorHEAD_base(
            self.req, _('Object'), self.obj_ring, partition,
            self.req.swift_entity_path)
        return resp.body

    def next(self):
        return self.__next__()