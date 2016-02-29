import os
import subprocess


def generate_rings():
    print (os.environ["PATH"])
    os.environ["PATH"] = '/home/mjwtom/install/python/bin' + ":" + os.environ["PATH"]
    print (os.environ["PATH"])

    ips = ['220.113.20.142', #'220.113.20.150' used for debug, so we have 12 nodes, I like 12 than 13
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

    dev = 'sdb1'
    ETC_SWIFT='/etc/swift'
    if not os.path.exists(ETC_SWIFT):
        os.makedirs(ETC_SWIFT)
    if os.path.exists(ETC_SWIFT+'/backups'):
        cmd = ['rm',
               '-rf',
              '%s/backups' % ETC_SWIFT]
        subprocess.call(cmd)
    print 'current work path:%s' % os.getcwd()
    os.chdir(ETC_SWIFT)
    print 'change work path to:%s' % os.getcwd()
    files = os.listdir(ETC_SWIFT)
    for file in files:
        path = ETC_SWIFT + '/' + file
        if os.path.isdir(path):
            continue
        shotname, extentsion = os.path.splitext(file)
        if (extentsion == '.builder') or (extentsion == '.gz'):
            try:
                os.remove(path)
            except Exception as e:
                print e

    for builder, port in [('object.builder', 6000),
                          ('object-1.builder', 6000),
                          ('object-2.builder', 6000),
                          ('container.builder', 6001),
                          ('account.builder', 6002)]:
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
        cmd = ['swift-ring-builder',
               '%s' % builder,
               'rebalance']
        subprocess.call(cmd)

if __name__ == '__main__':
    generate_rings()