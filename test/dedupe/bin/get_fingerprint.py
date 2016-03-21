from hashlib import md5
import sys
import os
from swift.dedupe.chunk import ChunkIterC
import pickle

target_size = 8192
fixed_size= False


def readfile(path, readsize=4096*1024):
    if not os.path.exists(path):
        print 'no such file'
    with open(path, 'rb') as f:
        data = f.read(readsize)
        while data:
            yield data
            data = f.read(readsize)
        else:
            return


def chunk_data(source, finger_file):
    if not os.path.exists(source):
        print 'no such file %s' % source
    data_source = readfile(source)
    chunker = ChunkIterC(data_src=data_source,
                         fixed_size=fixed_size,
                         target=target_size)
    for chunk in chunker:
        fp = md5(chunk).hexdigest()
        l = len(chunk)
        data = '%s %d\n' % (fp, l)
        finger_file.write(data)
    print 'finish file %s' % source


def get_finger(pickle_path, finger_path):
    if not os.path.exists(pickle_path):
        print 'no pickle file %s' % pickle_path
        return
    fin = open(pickle_path)
    inputfiles = pickle.load(fin)
    fin.close()
    finger_file = open(finger_path, 'w')
    for source in inputfiles:
        finger_file.write(source+':\n')
        chunk_data(source, finger_file)
    finger_file.close()


if True: #if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'please give the pickle file and fingerprint file'
        exit()
    pickle_name = sys.argv[1]
    finger_name = sys.argv[2]
    get_finger(pickle_name, finger_name)

