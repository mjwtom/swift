import six.moves.cPickle as pickle


class DedupeContainer(object):

    def __init__(self, id=None, size=4096):
        self.kv = dict()
        self.id = id
        self.size = size

    def add(self, fp, chunk):
        if len(self.kv) < self.size:
            self.kv[fp] = chunk
            return True
        else:
            return False

    def get(self, key):
        return self.kv.get(key, None)

    def is_full(self):
        return len(self.kv) >= self.size

    def size(self):
        return self.size

    def len(self):
        return len(self.kv)

    def dumps(self):
        data = pickle.dumps(self.kv)
        return data

    def get_id(self):
        return self.id

    def loads(self, data):
        self.kv = pickle.loads(data)
        self.size = len(self.kv)

    def get_fps_dict(self):
        fps_dict = {k:self.id for k in self.kv.keys()}
        return fps_dict

    def get_fps(self):
        fps = list(self.kv.keys())
        return fps

    def get_fps_lens(self):
        fps_lens = [(k, len(data)) for k, data in self.kv.items()]
        return fps_lens
