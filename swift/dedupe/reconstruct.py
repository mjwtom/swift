'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

from swift.proxy.controllers import base

class DedupRecon(object):
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