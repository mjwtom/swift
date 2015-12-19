'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import six.moves.cPickle as pickle
from swift.dedupe.dedupe_container import DedupeContainer
import lz4


class RespBodyIter(object):
    def __init__(self, req, controller):
        self.controller = controller
        self.object_name = controller.object_name
        self.dedupe = controller.dedupe
        self.req = req
        self.resp = controller.GETorHEAD(req)
        self.file_recipe = ''
        for d in iter(self.resp.app_iter):
            self.file_recipe += d
        if self.resp.headers.get('X-Object-Sysmeta-Compressed'):
            self.file_recipe = lz4.loads(self.file_recipe)
        self.file_recipe = pickle.loads(self.file_recipe)
        self.req_environ_path = self.req.environ['PATH_INFO']

    def __iter__(self):
        return self

    def __next__(self):
        if not self.file_recipe:
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fp, dc = self.file_recipe.pop(0)
        dedupe = self.dedupe
        dedupe.state.incre_download_chunk()

        if dc == str(dedupe.container_count):
            r = dedupe.container.kv.get(fp, None)
            if r:
                return r

        dc_container = dedupe.DCFromCache(dc)
        if dc_container:
            r = dc_container.kv.get(fp, None)
            if r:
                return r

        l = len(self.object_name)
        tmp_pth = self.req_environ_path[:-l]
        self.req.environ['PATH_INFO'] = tmp_pth+ str(dc)

        dc_container = DedupeContainer(dc)

        self.controller.object_name = dc
        resp = self.controller.GETorHEAD(self.req)
        data = ''
        for d in iter(resp.app_iter):
            data += d
        if resp.headers.get('X-Object-Sysmeta-Compressed'):
            data = lz4.loads(data)
        dc_container.loads(data)

        dedupe.DC2Cache(dc, dc_container)

        r = dc_container.kv.get(fp, None)
        if r:
            return r
        return None

    def next(self):
        return self.__next__()

    def get_resp(self):
        return self.resp