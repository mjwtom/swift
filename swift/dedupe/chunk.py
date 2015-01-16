'''
Created on Jan 4, 2015

@author: mjwtom
'''

from rabin_karp import Rabin_Karp
from docutils.nodes import target
from swift.common.exceptions import ChunkReadTimeout


class chunkIter(object):
    '''
    This class is to chunk the data string in to chunks based on cdc chunking method
    '''


    def __init__(self, data_src, win_size=48, min=512, max=16 * 1024, target=8192, MAGIC=13):
        '''
        TO chunk the data according the the rabin fingerprint
        '''
        # initial some environments
        self.buf = ''
        self.data_src = data_src
        self.win_size = 48
        self.min = min
        self.max = max
        self.target = target
        self.MAGIC = MAGIC
        self.left_len = 0

    def __iter__(self):
        return self

    def __next__(self):
        '''
        method to return a chunk each call
        '''
        # if the data size in the buffer is below the minimum chunk size, try to read data from data size
        if (len(self.buf) < self.min):
            try:
                self.buf = self.buf + next(self.data_src)
            except StopIteration:
                if (len(self.buf) > 0):
                    return self.buf
                raise StopIteration
            except ChunkReadTimeout:
                raise ChunkReadTimeout

        size = len(self.buf)
        parsed = 0

        #if the remained data size below the minimum chunk size, return this data as a data chunk
        if (size < self.min):
            return self.buf
        else:
            rabin = Rabin_Karp(self.buf[self.min - self.win_size:])
            parsed = self.min

        while True:
            if self.MAGIC == (rabin.digest() % self.target):
                buf = self.buf
                self.buf = buf[parsed:]
                return buf[:parsed]

            # if the buffer is exhausted, read data to the buffer
            if (parsed >= size):
                try:
                    buf = next(self.data_src)
                    self.buf = self.buf + buf
                    rabin.str = rabin.str + buf
                    size = len(self.buf)
                except StopIteration:
                    if (len(self.buf) > 0):
                        buf = self.buf
                        self.buf = buf[parsed:]
                        return buf
                    raise StopIteration
                except ChunkReadTimeout:
                    raise ChunkReadTimeout
            # reach the maximum chunk size, return this chunk
            if (self.max <= parsed):
                buf = self.buf
                self.buf = buf[parsed:]
                return buf[:parsed]
            rabin.update()
            parsed += 1

    def next(self):
        return self.__next__()
        