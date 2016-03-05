'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import six.moves.cPickle as pickle
import lz4
import zlib


class RespBodyIter(object):
    def __init__(self, chunk_store, req, controller):
        self.controller = controller
        self.req = req
        self.chunk_store = chunk_store
        self.resp = controller.GETorHEAD(req)
        self.file_recipe = ''
        for d in iter(self.resp.app_iter):
            self.file_recipe += d
        if self.resp.headers.get('X-Object-Sysmeta-Compressed'):
            method = self.resp.headers.get('X-Object-Sysmeta-CompressionMethod', 'lz4hc')
            if method == 'lz4hc' or method == 'lz4':
                self.file_recipe = lz4.loads(self.file_recipe)
            else:
                self.file_recipe = zlib.decompress(self.file_recipe)
        self.file_recipe = pickle.loads(self.file_recipe)
        self.req_environ_path = self.req.environ['PATH_INFO']

    def __iter__(self):
        return self

    def __next__(self):
        if not self.file_recipe:
            self.req.environ['PATH_INFO'] = self.req_environ_path
            raise StopIteration
        else:
            fp = self.file_recipe.pop(0)

        return self.chunk_store.get(fp, self.controller, self.req)


    def next(self):
        return self.__next__()

    def get_resp(self):
        return self.resp