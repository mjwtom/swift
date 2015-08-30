__author__ = 'mjwtom'

from hashlib import md5


class fingerprint(object):

    def __init__(self, data):
        self.hash=md5(data)

    def digest(self):
        return self.hash.digest()

    def hexdigest(self):
        return self.hash.hexdigest()