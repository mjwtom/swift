'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import six.moves.cPickle as pickle
from swift.dedupe.compress import decompress


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
            dedupe_start = self.chunk_store.summary.time()
            self.file_recipe = decompress(self.file_recipe, method)
            dedupe_end = self.chunk_store.summary.time()
            self.chunk_store.summary.decompression_time += chunk_store.summary.time_diff(dedupe_start, dedupe_end)
        self.file_recipe = pickle.loads(self.file_recipe)
        self.req_environ_path = self.req.environ['PATH_INFO']

    def __iter__(self):
        return self

    def __next__(self):
        if not self.file_recipe:
            self.req.environ['PATH_INFO'] = self.req_environ_path

            # log the information
            info = self.chunk_store.summary.get_info()
            for entry in info:
                self.chunk_store.logger.info(entry)
            self.chunk_store.log_message([self.controller.object_name])
            self.chunk_store.log_message(info)

            raise StopIteration
        else:
            fp = self.file_recipe.pop(0)

        return self.chunk_store.get(fp, self.controller, self.req)


    def next(self):
        return self.__next__()

    def get_resp(self):
        return self.resp