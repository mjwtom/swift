'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import os
import binascii
from swift import gettext_ as _
from swift.common.storage_policy import POLICIES


class RespBodyIter(object):
    def __init__(self, req, controller):
        self.controller = controller
        self.req = req
        container_info = controller.container_info(
            controller.account_name, controller.container_name, req)
        req.acl = container_info['read_acl']
        # pass the policy index to storage nodes via req header
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                       container_info['storage_policy'])
        policy = POLICIES.get_by_index(policy_index)
        obj_ring = controller.app.get_object_ring(policy_index)
        req.headers['X-Backend-Storage-Policy-Index'] = policy_index
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        partition = obj_ring.get_part(
            controller.account_name, controller.container_name, controller.object_name)
        node_iter = controller.app.iter_nodes(obj_ring, partition)
        resp = controller.GETorHEAD(req)
        self.fingerprints = resp.body
        # self.fp_number = len(self.fingerprints) / 16
        self.fp_number = len(self.fingerprints) / 32
        self.fp_cur = 0
        self.fp_size = 32
        self.obj_ring = obj_ring
        self.req_environ_path = self.req.environ['PATH_INFO']

    def __iter__(self):
        return self

    def __next_raw__(self):
        if(self.fp_cur >= self.fp_number):
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fingerprint = self.fingerprints[self.fp_cur*self.fp_size:self.fp_cur*self.fp_size+self.fp_size]
            self.fp_cur += 1
            str_fingerprint = binascii.hexlify(fingerprint)

        self.req.environ['PATH_INFO'] = os.path.dirname(self.req_environ_path)
        self.req.environ['PATH_INFO'] = self.req.environ['PATH_INFO'] + '/' + str_fingerprint

        partition = self.obj_ring.get_part(
            self.account_name, self.container_name, str_fingerprint)
        resp = self.GETorHEAD_base(
            self.req, _('Object'), self.obj_ring, partition,
            self.req.swift_entity_path)
        return resp.body

    def __next__(self):
        if(self.fp_cur >= self.fp_number):
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fingerprint = self.fingerprints[self.fp_cur*self.fp_size:self.fp_cur*self.fp_size+self.fp_size]
            self.fp_cur += 1

        self.req.environ['PATH_INFO'] = os.path.dirname(self.req_environ_path)
        self.req.environ['PATH_INFO'] = self.req.environ['PATH_INFO'] + '/' + fingerprint

        partition = self.obj_ring.get_part(
            self.controller.account_name, self.controller.container_name, fingerprint)
        resp = self.controller.GETorHEAD(self.req)
        return resp.body

    def next(self):
        return self.__next__()