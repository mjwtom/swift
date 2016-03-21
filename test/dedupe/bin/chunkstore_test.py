import sys


def get_size(finger_path):
    with open(finger_path, 'r') as f:
        line = f.readline()
        while line:
            name = line
            total_size = 0
            line = f.readline()
            while line and (not line.startswith('/home/')):
                size = line.split()
                size = int(size[1])
                total_size += size
                line = f.readline()
            else:
                print 'file %s has total length: %d' % (name, total_size)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'not enough arguments'
        exit()
    finger_path = sys.argv[1]
    get_size(finger_path)