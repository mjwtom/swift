from DedupeTest import DeduplicationTest
import sys
import os
import pickle


def scan_file(dir, pickle_file, min_size = 0):
    test = DeduplicationTest('/tmp/scan.txt')
    test.scan_dir(dir, pickle_file, min_size)
    files = test.get_files(pickle_file)
    test.print_files(files)


def scan_size(path, pickle_file, min_size = 0):
    def recur_scan_size(path, result, min_size):
        if os.path.isfile(path):
            size = os.path.getsize(path)
            info = dict(
                name = path,
                size = size
            )
            result.append(info)
        else:
            for f in os.listdir(path):
                subpath = os.path.join(path, f)
                recur_scan_size(subpath, result, min_size)
    result = []
    recur_scan_size(path, result, min_size)
    outf = open(pickle_file, 'wb')
    pickle.dump(result, outf)
    outf.close()
    return result



if __name__ == '__main__':
    if len(sys.argv) < 4:
        print 'not enough paramaters'
        exit()
    dir = sys.argv[2]
    pickle_file = sys.argv[3]
    if len(sys.argv) > 4:
        min_size = int(sys.argv[4])
    else:
        min_size = 0
    if sys.argv[1] == 'file':
        test = DeduplicationTest('/tmp/scan.txt')
        test.scan_dir(dir, pickle_file, min_size)

        files = test.get_files(pickle_file)
        test.print_files(files)
    if sys.argv[1] == 'size':
        files = scan_size(dir, pickle_file, min_size)
        print files