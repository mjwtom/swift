'''
Created on Jan 4, 2015

@author: mjwtom
'''


class Rabin_Karp:
    '''
    written by jingwei ma (mjwtom@gmail.com) to calculate rabin fingerprint
    '''


    def __init__(self, string, win_size=48, prime=257, mod=1000000007):
        '''
        Constructor
        '''
        # These are some typical initial values
        self.PRIME_BASE = prime
        self.PRIME_MOD = mod
        self.str = string
        self.POWER = 1
        for i in range(win_size):
            self.POWER = (self.POWER * self.PRIME_BASE) % self.PRIME_MOD

        self.init = 0
        self.end = win_size
        self.hash = 0
        for i in range(win_size):
            self.hash = self.hash * self.PRIME_BASE + ord(self.str[i])
            if self.hash < 0:
                self.hash = self.hash + self.PRIME_MOD

    def append(self, string):
        str = self.str[self.init:self.end]
        self.str = str + string
        self.end = self.end - self.init
        self.init = 0

    def digest(self):
        return self.hash

    def update(self):
        if self.end < len(self.str):
            self.hash = self.hash * self.PRIME_BASE + ord(self.str[self.end])
            self.hash = self.hash % self.PRIME_MOD
            self.hash = self.hash - ord(self.str[self.init]) * self.POWER
            if self.hash < 0:
                self.hash = self.hash + self.PRIME_MOD
            self.init += 1
            self.end += 1