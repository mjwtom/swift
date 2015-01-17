'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

from swift.proxy.controllers import base

class DedupRespBody(object):
    def __init__(self, obj_ring, account_name, container_name, object_name):
        self.obj_ring = obj_ring
        self.account_name = account_name
        self.container_name = container_name
        self.object_name = object_name

    def iter_resp(self, req):
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

    def __next__(self, req):
        if(self.fp_cur >= self.fp_number):
            raise StopIteration
        else:
            fingerprint = self.fingerprints[self.fp_number*16:self.fp_number*16+16]
            str_fingerprint = ''
            for a in fingerprint:
                str_fingerprint += hex(ord(a))[2:]

        partition = self.obj_ring.get_part(
            self.account_name, self.container_name, str_fingerprint)
        resp = self.GETorHEAD_base(
            req, _('Object'), self.obj_ring, partition,
            req.swift_entity_path)


    def next(self, req):
        return self.next(req)