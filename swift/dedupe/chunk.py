from swift.common.exceptions import ChunkReadTimeout


class RabinKarp:
    def __init__(self, win_size=48, prime=257, mod=1000000007):
        self.PRIME_BASE = prime
        self.PRIME_MOD = mod
        self.ring = [0]*win_size
        self.fp = 0
        self.POWER = self.PRIME_BASE**win_size%self.PRIME_MOD

    def update(self, ch):
        self.fp *= self.PRIME_BASE
        self.fp += ord(ch)
        self.ring.append(ord(ch))
        out = self.ring.pop(0)
        self.fp -= out*self.POWER%self.PRIME_MOD
        #in case self.fp is below zero, I donnot want to use if else
        self.fp += self.PRIME_MOD
        self.fp %= self.PRIME_MOD
        return self.fp

    def digest(self):
        return self.fp

    def append(self, data):
        for ch in data:
            self.update(ch)
        return self.fp


class ChunkIter(object):
    '''
    This class is to chunk the data string in to chunks based on cdc chunking method
    '''

    def __init__(self, data_src, fixed_size= False, target=8192, win_size=48, min=512, max=16 * 1024, MAGIC=13):
        '''
        TO chunk the data according the the rabin fingerprint
        '''
        # initial some environments
        self.buf = str()
        self.data_src = data_src
        self.fixed_size = fixed_size
        self.win_size = win_size
        self.min = min
        self.max = max
        self.target = target
        self.MAGIC = MAGIC
        self.left_len = 0

    def __iter__(self):
        return self

    def get_fixed(self):
        while len(self.buf) < self.target:
            try:
                self.buf = self.buf + next(self.data_src)
            except StopIteration:
                if len(self.buf) > 0:
                    buf = self.buf
                    self.buf = str()
                    return buf
                raise StopIteration
            except ChunkReadTimeout:
                raise ChunkReadTimeout
        buf = self.buf[:self.target]
        self.buf = self.buf[self.target:]
        return buf

    def get_variable(self):
        '''
        method to return a chunk each call
        '''
        # if the data size in the buffer is below the minimum chunk size, try to read data from data size
        while len(self.buf) < self.min:
            try:
                self.buf = self.buf + next(self.data_src)
            except StopIteration:
                if len(self.buf) > 0:
                    buf = self.buf
                    self.buf = str()
                    return buf
                raise StopIteration
            except ChunkReadTimeout:
                raise ChunkReadTimeout

        rabin = RabinKarp(win_size=self.win_size)
        cur = self.min
        fp = rabin.append(self.buf[cur-self.win_size:cur])

        while True:
            while cur < len(self.buf):
                if self.MAGIC == (fp % self.target) or cur >= self.max:
                    buf = self.buf[:cur]
                    self.buf = self.buf[cur:]
                    return buf
                fp = rabin.update(self.buf[cur])
                cur += 1
            try:
                self.buf = self.buf + next(self.data_src)
            except StopIteration:
                if len(self.buf) > 0:
                    buf = self.buf
                    self.buf = str()
                    return buf
                raise StopIteration
            except ChunkReadTimeout:
                raise ChunkReadTimeout

    def __next__(self):
        if self.fixed_size:
            return self.get_fixed()
        else:
            return self.get_variable()

    def next(self):
        return self.__next__()