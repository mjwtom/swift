
import os
import subprocess

ip = '127.0.0.1'
SWIFT_ETC_DIR='/etc/swift/'
DATA_DIR='/home/mjwtom/swift-data/'

print (os.environ["PATH"])
os.environ["PATH"] = '/home/mjwtom/install/python/bin' + ":" + os.environ["PATH"]
print (os.environ["PATH"])

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

for builder, last_port_dig in [('object.builder', 0),
                           ('object-1.builder', 0),
                           ('object-2.builder', 0),
                           ('container.builder', 1),
                           ('account.builder', 2)]:

    cmd = ['swift-ring-builder',
           '%s' % builder,
           'create',
           '10',
           '3',
           '1']
    print 'cmd'.join(cmd)
    subprocess.call(cmd)

    for i in range(1, 5, 1):
        cmd = ['swift-ring-builder',
               '%s' % builder,
               'add',
               'r%dz%d-127.0.0.1:60%d%d/sdb%d' % (i, i, i, last_port_dig, i),
               '1']
        subprocess.call(cmd)
        device_dir = DATA_DIR+('/%d/sdb%d' % (i, i))
        if not os.path.exists(device_dir):
            os.makedirs(device_dir)
        i += 1
    os.system('chown -R mjwtom:mjwtom %s' % DATA_DIR)

    cmd = ['swift-ring-builder',
           '%s' % builder,
           'rebalance']
    print 'cmd'.join(cmd)
    subprocess.call(cmd)
