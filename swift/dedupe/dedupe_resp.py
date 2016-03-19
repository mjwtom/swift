'''
TODO: Read the fingerprints from the object file
according to the fingerprints. Read the corresponding data chunks to construct the data stream
'''

import six.moves.cPickle as pickle
from swift.dedupe.compress import decompress
from swift.dedupe.time import time, time_diff


class RespBodyIterOld(object):
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
        self.last_time = time()

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
            now = time()
            self.chunk_store.summary.iter_req_time += time_diff(self.last_time, now)
            fp = self.file_recipe.pop(0)
            data = self.chunk_store.get(fp, self.controller, self.req)
            self.last_time = time()
            return data


    def next(self):
        return self.__next__()

    def get_resp(self):
        return self.resp


class RespBodyIter(object):
    def __init__(self, chunk_store, req, controller):
        dedupe_start = time()
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
        dedupe_end = time()
        self.chunk_store.summary.get_recipe_time += time_diff(dedupe_start, dedupe_end)

    def iter_chunk(self):
        for fp in self.file_recipe:
            dedupe_start = time()
            data = self.chunk_store.get(fp, self.controller, self.req)
            dedupe_end = time()
            self.chunk_store.summary.get_chunk_time += time_diff(dedupe_start, dedupe_end)
            yield data

        #restore the information
        self.req.environ['PATH_INFO'] = self.req_environ_path

        # log the information
        info = self.chunk_store.summary.get_info()
        for entry in info:
            self.chunk_store.logger.info(entry)
        self.chunk_store.log_message([self.controller.object_name])
        self.chunk_store.log_message(info)
        return


def segment(chunk_iter, size=16):
    seg = []
    for chunk in chunk_iter.iter_chunk():
        seg.append(chunk)
        if len(seg) >= size:
            data = ''.join(seg)
            seg = []
            yield data
    data = ''.join(seg)
    yield data
    return

