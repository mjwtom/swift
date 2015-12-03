class DataIter(object):
    def __init__(self, data, chunk_size=1024*1024*1024):
        self.data = data
        self.chunk_size = chunk_size

    def __iter__(self):
        return self

    def __next__(self):
        if not self.data:
            raise StopIteration
        if len(self.data) > self.chunk_size:
            d = self.data[:self.chunk_size]
            self.data = self.data[self.chunk_size:]
            return d
        else:
            d = self.data
            self.data = ''
            return d

    def next(self):
        return self.__next__()