import sys
from test.dedupe.bin.DedupeTest import DeduplicationTest
import os
import pickle
from time import sleep


def test_upload(pickle_file, result_path, tmp_dir):
    if not os.path.exists(pickle_file):
        print 'no pickle  file to read: %s' % pickle_file
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    log_file = os.path.join(result_path, 'log.txt')

    test = DeduplicationTest(log_file)
    files = test.get_files(pickle_file)
    result, uploaded_files = test.uploads(files)
    uploaded_pickle = os.path.join(result_path, 'upload_result.pickle')
    out = open(uploaded_pickle, 'wb')
    pickle.dump(result, out)
    out.close()


def test_sequential_download(pickle_file, result_path, tmp_dir):
    if not os.path.exists(pickle_file):
        print 'no pickle  file to read: %s' % pickle_file
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    log_file = os.path.join(result_path, 'log.txt')

    test = DeduplicationTest(log_file)
    files = test.get_files(pickle_file)
    files = [file[1:] for file in files if file.startswith('/')]
    result, download_files = test.sequential_download(files)
    seq_download_pickle = os.path.join(result_path, 'seq_download_result.pickle')
    out = open(seq_download_pickle, 'wb')
    pickle.dump(result, out)
    out.close()


def test_random_download(pickle_file, result_path, tmp_dir):
    if not os.path.exists(pickle_file):
        print 'no pickle  file to read: %s' % pickle_file
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    log_file = os.path.join(result_path, 'log.txt')

    test = DeduplicationTest(log_file)
    files = test.get_files(pickle_file)
    files = [file[1:] for file in files if file.startswith('/')]
    result, download_files = test.sequential_download(files)
    rnd_download_pickle = os.path.join(result_path, 'rnd_download_result.pickle')
    out = open(rnd_download_pickle, 'wb')
    pickle.dump(result, out)
    out.close()


def test_upload_download(pickle_file, result_path):
    if not os.path.exists(pickle_file):
        print 'no pickle  file to read: %s' % pickle_file
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    log_file = os.path.join(result_path, 'log.txt')

    print 'start to upload'
    test = DeduplicationTest(log_file)
    files = test.get_files(pickle_file)

    result, uploaded_files = test.uploads(files)
    uploaded_pickle = os.path.join(result_path, 'upload_result.pickle')
    out = open(uploaded_pickle, 'wb')
    pickle.dump(result, out)
    out.close()

    sleep(60)

    uploaded_files = [file[1:] for file in uploaded_files]
    result, download_files = test.sequential_download(uploaded_files, 30)
    seq_download_pickle = os.path.join(result_path, 'seq_download_result.pickle')
    out = open(seq_download_pickle, 'wb')
    pickle.dump(result, out)
    out.close()

    sleep(60)

    rnd_download_pickle = os.path.join(result_path, 'rnd_download_result.pickle')
    result, download_files = test.random_download(uploaded_files, 30)
    out = open(rnd_download_pickle, 'wb')
    pickle.dump(result, out)
    out.close()


if __name__=='__main__':
    if len(sys.argv) < 3:
        print 'pleas give the case to test'
        exit()
    pickle_path = sys.argv[1]
    result_path = sys.argv[2]
    print 'uploads file in %s, results saves in %s' % (pickle_path, result_path)
    test_upload_download(pickle_path, result_path)
