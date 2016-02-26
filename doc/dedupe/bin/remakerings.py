
import os
import subprocess

ips = ['220.113.20.150',
       '220.113.20.142',
       '220.113.20.144',
       '220.113.20.151',
       '220.113.20.120',
       '220.113.20.121',
       '220.113.20.122',
       '220.113.20.123',
       '220.113.20.124',
       '220.113.20.127',
       '220.113.20.128',
       '220.113.20.129',
       '220.113.20.131']
port = 6000
dev = '/home/m/mjwtom/swift-data/'
SWIFT_ETC_DIR='/etc/swift/'

print os.getcwd()
os.chdir(SWIFT_ETC_DIR)

files = os.listdir(SWIFT_ETC_DIR)
for file in files:
    path = SWIFT_ETC_DIR + '/' + file
    if os.path.isdir(path):
        continue
    shotname, extentsion = os.path.splitext(file)
    if (extentsion == 'builder') or (extentsion == '.gz'):
        os.remove(path)

for builder in ['object.builder',
                'object-1.builder',
                'object-2.builder',
                'container-builder',
                'account-builder']:

    cmd = ['swift-ring-builder',
           '%s' % builder,
           'create',
           '10',
           '3',
           '1']
    subprocess.call(cmd)
    i = 1
    for ip in ips:
        cmd = ['swift-ring-builder',
               '%s' % builder,
               'add',
               'r%dz%d-%s:%d/%s' % (i, i, ip, port, dev),
               '1']
        subprocess.call(cmd)
        i += 1

    cmd = cmd = ['swift-ring-builder',
           '%s' % builder,
           'rebalance']
    subprocess.call(cmd)