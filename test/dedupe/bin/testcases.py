import sys
from test.dedupe.bin.DedupeTest import DeduplicationTest
import os
import pickle
from copy import copy


log_file_normal = '/home/m/mjwtom/test/vm-normal.txt'
pickle_vm = '/home/m/mjwtom/test/vm.pickle'

tmp_dir = '/home/m/mjwtom/tmp'

normal_upload = '/home/m/mjwtom/test/vm-normal-upload-result.pickle'
seq_download = '/home/m/mjwtom/test/vm-normal-sequetianl-download-result.pickle'
rnd_download = '/home/m/mjwtom/test/vm-normal-random-download-result.pickle'


def test_normal():
    dir, file = os.path.split(log_file_normal)
    if not os.path.exists(dir):
        os.makedirs(dir)
    test = DeduplicationTest(log_file_normal)
    files = test.get_files(pickle_vm)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    result = test.fetch_upload(files, tmp_dir)
    out = open(normal_upload, 'wb')
    pickle.dump(result, out)
    out.close()

    uploaded_files = []
    for info in result:
        uploaded_files.append(info.get('file'))

    uploaded_files_2 = copy(uploaded_files)

    result = test.sequential_download(uploaded_files)
    out = open(seq_download, 'wb')
    pickle.dump(result, out)
    out.close()

    result = test.random_download(uploaded_files_2)
    out = open(rnd_download, 'wb')
    pickle.dump(result, out)
    out.close()




if __name__=='__main__':
    case = sys.argv[1]
    if case == 'normal':
        test_normal()