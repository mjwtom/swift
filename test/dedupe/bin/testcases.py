import sys
from test.dedupe.bin.DedupeTest import DeduplicationTest
import os
import pickle


log_file_normal = '/home/m/mjwtom/test/vm-normal.txt'
pickle_vm = '/home/m/mjwtom/test/vm.pickle'

tmp_dir = '/home/m/mjwtom/tmp'

normal_upload = '/home/m/mjwtom/test/vm-normal-upload-result.pickle'
seq_download = '/home/m/mjwtom/test/vm-normal-sequetianl-download-result.pickle'
rnd_download = '/home/m/mjwtom/test/vm-normal-random-download-result.pickle'

####
'''
log_file_normal = '/home/mjwtom/test/vm-normal.txt'
pickle_vm = '/home/mjwtom/test/vm.pickle'

tmp_dir = '/home/mjwtom/tmp'

normal_upload = '/home/mjwtom/test/vm-normal-upload-result.pickle'
seq_download = '/home/mjwtom/test/vm-normal-sequetianl-download-result.pickle'
rnd_download = '/home/mjwtom/test/vm-normal-random-download-result.pickle'
'''


def test_normal():

    dir, file = os.path.split(log_file_normal)
    if not os.path.exists(dir):
        os.makedirs(dir)
    test = DeduplicationTest(log_file_normal)
    files = test.get_files(pickle_vm)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    result, uploaded_files = test.fetch_upload(files, tmp_dir)
    out = open(normal_upload, 'wb')
    pickle.dump(result, out)
    out.close()

    '''
    f = open(normal_upload, 'rb')
    result = pickle.load(f)
    f.close()
    uploaded_files = [info['file'][1:] for info in result]
    test = DeduplicationTest(log_file_normal)
    '''

    uploaded_files = [file[1:] for file in uploaded_files]
    result, download_files = test.sequential_download(uploaded_files)
    out = open(seq_download, 'wb')
    pickle.dump(result, out)
    out.close()

    result, download_files = test.random_download(uploaded_files)
    out = open(rnd_download, 'wb')
    pickle.dump(result, out)
    out.close()




if __name__=='__main__':
    if len(sys.argv) < 2:
        print 'pleas give the case to test'
        exit()
    case = sys.argv[1]
    if case == 'normal':
        test_normal()
