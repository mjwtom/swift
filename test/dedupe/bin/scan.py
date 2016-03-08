from DedupeTest import DeduplicationTest
import sys


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'not enough paramaters'
        exit()
    dir = sys.argv[1]
    pickle_file = sys.argv[2]
    test = DeduplicationTest('/tmp/scan.txt')
    test.scan_dir(dir, pickle_file)

    files = test.get_files(pickle_file)
    test.print_files(files)