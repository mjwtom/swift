from struct import pack, unpack, calcsize

class dedupe_container(object):

    def __init__(self, name=None, size=4096):
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

    def tobyte(self):
        data = ''
        l = len(self.name)
        fmt = 'i%ds' % l
        data += pack(fmt, l, self.name)
        fmt = 'ii'
        data += pack(fmt, self.len, self.size)
        for i in range(0, self.len):
            f = self.fp[i]
            l = len(f)
            fmt = 'i%ds' % l
            data += pack(fmt, l, f)
            chunk = self.kv[f]
            l = len(chunk)
            fmt = 'i%ds' % l
            data += pack(fmt, l, chunk)
        return data

    def frombyte(self, data):
        ll = calcsize('i')
        t = data[:ll]
        data = data[ll:]
        l, = unpack('i', t)
        fmt = '%ds' % l
        ll = calcsize(fmt)
        t = data[:ll]
        data = data[ll:]
        self.name, = unpack(fmt, t)
        ll = calcsize('ii')
        t = data[:ll]
        data = data[ll:]
        self.len, self.size = unpack('ii', t)
        for i in range(0, self.len):
            ll = calcsize('i')
            t = data[:ll]
            data = data[ll:]
            l, = unpack('i', t)
            fmt = '%ds' % l
            ll = calcsize(fmt)
            t = data[:ll]
            data = data[ll:]
            fp, = unpack(fmt, t)
            self.fp.append(fp)
            ll = calcsize('i')
            t = data[:ll]
            data = data[ll:]
            l, = unpack('i', t)
            fmt = '%ds' % l
            ll = calcsize(fmt)
            t = data[:ll]
            data = data[ll:]
            chunk, = unpack(fmt, t)
            self.kv[fp] = chunk



'''
# test
c = dedupe_container('hello', 4096)
for i in range(0, 1000):
    fp = str(i)
    chunk = '0000000000'
    c.add(fp, chunk)


for i in range(0, c.len):
    fp = c.fp[i]
    print('%s\n', fp)
    print('%s\n', c.kv[fp])

str = c.tobyte()

d = dedupe_container('hi', 4096)

d.frombyte(str)

for i in range(0, d.len):
    fp = d.fp[i]
    print('%s\n', fp)
    print('%s\n', d.kv[fp])

'''
