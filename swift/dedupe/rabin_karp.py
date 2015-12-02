'''
Created on Jan 4, 2015

@author: mjwtom
'''


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