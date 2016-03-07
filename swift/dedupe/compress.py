import lz4
import zlib


def compress(data, method=None):
    if method == 'zlib':
        data = zlib.compress(data)
    elif method == 'lz4':
        data = lz4.dumps(data)
    else:
        data = lz4.compressHC(data)
    return data

def decompress(data, method=None):
    if method == 'zlib':
        data = zlib.decompress(data)
    else:
        data = lz4.loads(data)
    return data