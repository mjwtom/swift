

class dedupe_container(object):

    def __init__(self, name, size):
        self.name = name
        self.size = size
        self.kv = {}
        self.fp = []
        self.len = 0

    def add(self, fp, chunk):
        if(self.len < self.size):
            self.kv[fp] = chunk
            self.fp.append(fp)
            self.len += 1
            return True
        else:
            return False

    def get(self, key):
        return self.kv[key]

    def is_full(self):
        if self.len >= self.size:
            return True
        else:
            return False

    def size(self):
        return self.len