import six.moves.cPickle as pickle


class DedupeContainer(object):

    def __init__(self, name=None, size=4096):
        self.kv = {'name': name, 'size': size, 'len': 0}

    def add(self, fp, chunk):
        if self.kv['len'] < self.kv['size']:
            self.kv[fp] = chunk
            self.kv['len'] += 1
            return True
        else:
            return False

    def get(self, key):
        return self.kv.get(key, None)

    def is_full(self):
        len = self.kv.get('len', None)
        size = self.kv.get('size', None)
        return len >= size

    def size(self):
        return self.kv.get('size', None)

    def len(self):
        return self.kv.get('len', None)

    def dumps(self):
        data = pickle.dumps(self.kv)
        return data

    def get_name(self):
        name = self.kv.get('name', None)
        return name

    def loads(self, data):
        self.kv = pickle.loads(data)
